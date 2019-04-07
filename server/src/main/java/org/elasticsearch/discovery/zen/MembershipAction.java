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

package org.elasticsearch.discovery.zen;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

//Structure:节点关系操作类（用于接收其他节点的加入和离开请求 或 向主节点发送加入或离开请求）
//Michel：https://github.com/mushao999/elasticsearch_note/blob/master/server/elasticsearch/discover/zen/MembershipAction.md
public class MembershipAction extends AbstractComponent {

    public static final String DISCOVERY_JOIN_ACTION_NAME = "internal:discovery/zen/join";
    public static final String DISCOVERY_JOIN_VALIDATE_ACTION_NAME = "internal:discovery/zen/join/validate";
    public static final String DISCOVERY_LEAVE_ACTION_NAME = "internal:discovery/zen/leave";

    public interface JoinCallback {
        void onSuccess();

        void onFailure(Exception e);
    }
    //Michel:抽象一个加入和离开集群的监听器，用于对外暴露有节点加入和离开时执行的动作
    public interface MembershipListener {
        void onJoin(DiscoveryNode node, JoinCallback callback);

        void onLeave(DiscoveryNode node);
    }

    private final TransportService transportService;

    private final MembershipListener listener;
    //Michel:注册join validate leave三种请求对应的处理handler
    public MembershipAction(Settings settings, TransportService transportService, MembershipListener listener,
                            Collection<BiConsumer<DiscoveryNode,ClusterState>> joinValidators) {
        super(settings);
        this.transportService = transportService;
        this.listener = listener;


        transportService.registerRequestHandler(DISCOVERY_JOIN_ACTION_NAME, JoinRequest::new,
            ThreadPool.Names.GENERIC, new JoinRequestRequestHandler());
        transportService.registerRequestHandler(DISCOVERY_JOIN_VALIDATE_ACTION_NAME,
            () -> new ValidateJoinRequest(), ThreadPool.Names.GENERIC,
            new ValidateJoinRequestRequestHandler(transportService::getLocalNode, joinValidators));
        transportService.registerRequestHandler(DISCOVERY_LEAVE_ACTION_NAME, LeaveRequest::new,
            ThreadPool.Names.GENERIC, new LeaveRequestRequestHandler());
    }
    //Michel:发送离开集群请求
    public void sendLeaveRequest(DiscoveryNode masterNode, DiscoveryNode node) {
        transportService.sendRequest(node, DISCOVERY_LEAVE_ACTION_NAME, new LeaveRequest(masterNode),
            EmptyTransportResponseHandler.INSTANCE_SAME);
    }
    //Michel:阻塞发起离开集群请求
    public void sendLeaveRequestBlocking(DiscoveryNode masterNode, DiscoveryNode node, TimeValue timeout) {
        transportService.submitRequest(masterNode, DISCOVERY_LEAVE_ACTION_NAME, new LeaveRequest(node),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }
    //Michel:阻塞发起加入集群请求
    public void sendJoinRequestBlocking(DiscoveryNode masterNode, DiscoveryNode node, TimeValue timeout) {
        transportService.submitRequest(masterNode, DISCOVERY_JOIN_ACTION_NAME, new JoinRequest(node),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Validates the join request, throwing a failure if it failed.
     */
    //Michel:阻塞发起加入集群验证请求
    public void sendValidateJoinRequestBlocking(DiscoveryNode node, ClusterState state, TimeValue timeout) {
        transportService.submitRequest(node, DISCOVERY_JOIN_VALIDATE_ACTION_NAME, new ValidateJoinRequest(state),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }
    //Michel:加入集群请求
    public static class JoinRequest extends TransportRequest {

        DiscoveryNode node;

        public JoinRequest() {
        }

        private JoinRequest(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            node = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            node.writeTo(out);
        }
    }

    //Michel:加入集群请求处理handler
    private class JoinRequestRequestHandler implements TransportRequestHandler<JoinRequest> {

        @Override
        public void messageReceived(final JoinRequest request, final TransportChannel channel) throws Exception {
            listener.onJoin(request.node, new JoinCallback() {
                @Override
                public void onSuccess() {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed to send back failure on join request", inner);
                    }
                }
            });
        }
    }
    //Michel:验证加入集群请求
    static class ValidateJoinRequest extends TransportRequest {
        private ClusterState state;

        ValidateJoinRequest() {}

        ValidateJoinRequest(ClusterState state) {
            this.state = state;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.state = ClusterState.readFrom(in, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.state.writeTo(out);
        }
    }
    //Michel:验证加入集群处理Handler
    static class ValidateJoinRequestRequestHandler implements TransportRequestHandler<ValidateJoinRequest> {
        private final Supplier<DiscoveryNode> localNodeSupplier;
        private final Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators;

        ValidateJoinRequestRequestHandler(Supplier<DiscoveryNode> localNodeSupplier,
                                          Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators) {
            this.localNodeSupplier = localNodeSupplier;
            this.joinValidators = joinValidators;
        }

        @Override
        public void messageReceived(ValidateJoinRequest request, TransportChannel channel) throws Exception {
            DiscoveryNode node = localNodeSupplier.get();
            assert node != null : "local node is null";
            joinValidators.stream().forEach(action -> action.accept(node, request.state));
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    /**
     * Ensures that all indices are compatible with the given node version. This will ensure that all indices in the given metadata
     * will not be created with a newer version of elasticsearch as well as that all indices are newer or equal to the minimum index
     * compatibility version.
     * @see Version#minimumIndexCompatibilityVersion()
     * @throws IllegalStateException if any index is incompatible with the given version
     */
    //Michel:索引兼容性检查validator
    //Michel:索引兼容性检查：索引creationVersion必须在节点的minimumIndexCompatibilityVersion和version之间
    static void ensureIndexCompatibility(final Version nodeVersion, MetaData metaData) {
        Version supportedIndexVersion = nodeVersion.minimumIndexCompatibilityVersion();
        // we ensure that all indices in the cluster we join are compatible with us no matter if they are
        // closed or not we can't read mappings of these indices so we need to reject the join...
        for (IndexMetaData idxMetaData : metaData) {
            if (idxMetaData.getCreationVersion().after(nodeVersion)) {
                throw new IllegalStateException("index " + idxMetaData.getIndex() + " version not supported: "
                    + idxMetaData.getCreationVersion() + " the node version is: " + nodeVersion);
            }
            if (idxMetaData.getCreationVersion().before(supportedIndexVersion)) {
                throw new IllegalStateException("index " + idxMetaData.getIndex() + " version not supported: "
                    + idxMetaData.getCreationVersion() + " minimum compatible index version is: " + supportedIndexVersion);
            }
        }
    }

    /** ensures that the joining node has a version that's compatible with all current nodes*/
    //Michel:保证要加入集群的节点与集群中现有的其他节点版本兼容，作为加入的一个validator使用
    static void ensureNodesCompatibility(final Version joiningNodeVersion, DiscoveryNodes currentNodes) {
        final Version minNodeVersion = currentNodes.getMinNodeVersion();
        final Version maxNodeVersion = currentNodes.getMaxNodeVersion();
        ensureNodesCompatibility(joiningNodeVersion, minNodeVersion, maxNodeVersion);
    }

    /** ensures that the joining node has a version that's compatible with a given version range */
    //Michel:保证指定版本与第二个和第三个版本分别兼容
    static void ensureNodesCompatibility(Version joiningNodeVersion, Version minClusterNodeVersion, Version maxClusterNodeVersion) {
        //Grammar:assert condition:string
        assert minClusterNodeVersion.onOrBefore(maxClusterNodeVersion) : minClusterNodeVersion + " > " + maxClusterNodeVersion;
        if (joiningNodeVersion.isCompatible(maxClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "The cluster contains nodes with version [" + maxClusterNodeVersion + "], which is incompatible.");
        }
        if (joiningNodeVersion.isCompatible(minClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported." +
                "The cluster contains nodes with version [" + minClusterNodeVersion + "], which is incompatible.");
        }
    }

    /**
     * ensures that the joining node's major version is equal or higher to the minClusterNodeVersion. This is needed
     * to ensure that if the master is already fully operating under the new major version, it doesn't go back to mixed
     * version mode
     **/
    //Michel:确保加入的节点的主版本不小于当前规定的最小版本的主版本
    static void ensureMajorVersionBarrier(Version joiningNodeVersion, Version minClusterNodeVersion) {
        final byte clusterMajor = minClusterNodeVersion.major;
        if (joiningNodeVersion.major < clusterMajor) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "All nodes in the cluster are of a higher major [" + clusterMajor + "].");
        }
    }
    //Michel:节点离开集群请求
    // Question:异常挂掉不会有这个请求，是否有什么影响？
    public static class LeaveRequest extends TransportRequest {

        private DiscoveryNode node;

        public LeaveRequest() {
        }

        private LeaveRequest(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            node = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            node.writeTo(out);
        }
    }
    //Michel:处理节点离开集群请求handler
    private class LeaveRequestRequestHandler implements TransportRequestHandler<LeaveRequest> {

        @Override
        public void messageReceived(LeaveRequest request, TransportChannel channel) throws Exception {
            listener.onLeave(request.node);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }
}
