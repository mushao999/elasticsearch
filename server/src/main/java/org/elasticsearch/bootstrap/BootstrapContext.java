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
package org.elasticsearch.bootstrap;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;

/**
 * Context that is passed to every bootstrap check to make decisions on.
 * Michel:启动检查上下文，包括配置和元数据,主要用于启动check
 * Done
 */
public class BootstrapContext {
    /**
     * The nodes settings
     */
    public final Settings settings;
    /**
     * The nodes local state metadata loaded on startup
     */
    public final MetaData metaData;

    public BootstrapContext(Settings settings, MetaData metaData) {
        this.settings = settings;
        this.metaData = metaData;
    }
}
