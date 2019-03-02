/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.single.SingleNodeDiscovery;
import org.elasticsearch.discovery.zen.FileBasedUnicastHostsProvider;
import org.elasticsearch.discovery.zen.SettingsBasedHostsProvider;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A module for loading classes for node discovery.
 * Michel:加载节点发现功能的模块
 */
public class DiscoveryModule {
    private static final Logger logger = LogManager.getLogger(DiscoveryModule.class);

    public static final Setting<String> DISCOVERY_TYPE_SETTING =
        new Setting<>("discovery.type", "zen", Function.identity(), Property.NodeScope);//Michel:发现类型，默认为zen
    public static final Setting<List<String>> DISCOVERY_HOSTS_PROVIDER_SETTING =
        Setting.listSetting("discovery.zen.hosts_provider", Collections.emptyList(), Function.identity(), Property.NodeScope);//Michel:hosts提供者

    private final Discovery discovery;

    public DiscoveryModule(Settings settings, ThreadPool threadPool, TransportService transportService,
                           NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService, MasterService masterService,
                           ClusterApplier clusterApplier, ClusterSettings clusterSettings, List<DiscoveryPlugin> plugins,
                           AllocationService allocationService, Path configFile) {
        //Michel: step 1 生成hostProvider
        //Michel:   Step 1.1 生成所有可支持的hostProviders列表
        final Collection<BiConsumer<DiscoveryNode,ClusterState>> joinValidators = new ArrayList<>();
        final Map<String, Supplier<UnicastHostsProvider>> hostProviders = new HashMap<>();
        //Michel: 内置的settings和file hostProvider
        hostProviders.put("settings", () -> new SettingsBasedHostsProvider(settings, transportService));
        hostProviders.put("file", () -> new FileBasedUnicastHostsProvider(settings, configFile));
        //Michel: 插件中定义的hostProvider
        for (DiscoveryPlugin plugin : plugins) {
            plugin.getZenHostsProviders(transportService, networkService).entrySet().forEach(entry -> {
                //Grammar:Map的put方法会返回之前的值
                if (hostProviders.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Cannot register zen hosts provider [" + entry.getKey() + "] twice");
                }
            });
            BiConsumer<DiscoveryNode, ClusterState> joinValidator = plugin.getJoinValidator();
            if (joinValidator != null) {//Michel:插件定义的节点加入验证器，用于验证节点当前状态是否可以加入集群
                joinValidators.add(joinValidator);
            }
        }
        // Michel:  step 1.2 根据配置从hostProviders列表中选择要使用的hostProviders
        //Grammar: 此处生成的list可能不能直接添加元素，因此为了能添加元素需要转换为arrayList
        List<String> hostsProviderNames = DISCOVERY_HOSTS_PROVIDER_SETTING.get(settings);
        //Michel: 为了向后兼容，即使没有提供settings的provider，也添加上
        // for bwc purposes, add settings provider even if not explicitly specified
        if (hostsProviderNames.contains("settings") == false) {
            List<String> extendedHostsProviderNames = new ArrayList<>();
            extendedHostsProviderNames.add("settings");
            extendedHostsProviderNames.addAll(hostsProviderNames);
            hostsProviderNames = extendedHostsProviderNames;
        }
        //Michel: 查看配置中指定的provider是否有未提供实现的
        final Set<String> missingProviderNames = new HashSet<>(hostsProviderNames);
        missingProviderNames.removeAll(hostProviders.keySet());
        if (missingProviderNames.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown zen hosts providers " + missingProviderNames);
        }
        //Grammar: 双冒号操作符表示将该类的方法作为lamda参数传入
        //Grammar: Collection.stream()为当前集合生成流式处理对象，方便处理
        //Michel:从names获取provider的supplier列表，再从supplier中获取provider
        //Michel: 获取最终要使用的hostProviders
        List<UnicastHostsProvider> filteredHostsProviders = hostsProviderNames.stream()
            .map(hostProviders::get).map(Supplier::get).collect(Collectors.toList());
        //Question：此处会出现重复的host吧？
        //Michel: 将多个providers合并为一个provider
        final UnicastHostsProvider hostsProvider = hostsResolver -> {
            final List<TransportAddress> addresses = new ArrayList<>();
            for (UnicastHostsProvider provider : filteredHostsProviders) {
                addresses.addAll(provider.buildDynamicHosts(hostsResolver));
            }
            return Collections.unmodifiableList(addresses);
        };
        //Michel: step 2 获取发现方式
        //Michel:   step 2.1 生成所有支持的发现方式列表(使用第一步生成的hostProvider)
        Map<String, Supplier<Discovery>> discoveryTypes = new HashMap<>();
        //Michel: 加入内置的zen和setting发现方式
        discoveryTypes.put("zen",
            () -> new ZenDiscovery(settings, threadPool, transportService, namedWriteableRegistry, masterService, clusterApplier,
                clusterSettings, hostsProvider, allocationService, Collections.unmodifiableCollection(joinValidators)));
        discoveryTypes.put("single-node", () -> new SingleNodeDiscovery(settings, transportService, masterService, clusterApplier));
        //Michel: 加入插件中自定义的发现方式
        for (DiscoveryPlugin plugin : plugins) {
            plugin.getDiscoveryTypes(threadPool, transportService, namedWriteableRegistry,
                masterService, clusterApplier, clusterSettings, hostsProvider, allocationService).entrySet().forEach(entry -> {
                if (discoveryTypes.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Cannot register discovery type [" + entry.getKey() + "] twice");
                }
            });
        }
        //Michel:   step 2.2 获取配置中指定的发现方式
        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);
        Supplier<Discovery> discoverySupplier = discoveryTypes.get(discoveryType);
        if (discoverySupplier == null) {
            throw new IllegalArgumentException("Unknown discovery type [" + discoveryType + "]");
        }
        logger.info("using discovery type [{}] and host providers {}", discoveryType, hostsProviderNames);
        discovery = Objects.requireNonNull(discoverySupplier.get());
    }

    //Michel:获取生成的discovery
    public Discovery getDiscovery() {
        return discovery;
    }

}
