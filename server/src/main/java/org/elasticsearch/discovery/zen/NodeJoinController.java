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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

/**
 * This class processes incoming join request (passed zia {@link ZenDiscovery}). Incoming nodes
 * are directly added to the cluster state or are accumulated during master election.
 * Michel:该类用于处理其他节点的加入请求。收到的加入请求被积累起来（当前节点主节点角色没有被确认之前）或直接被添加到集群状态中
 */
//
public class NodeJoinController extends AbstractComponent {

    private final MasterService masterService;//Michel:更新集群状态使用
    private final JoinTaskExecutor joinTaskExecutor;//Michel:加入集群任务执行器

    // this is set while trying to become a master
    // mutation should be done under lock
    private ElectionContext electionContext = null;


    public NodeJoinController(MasterService masterService, AllocationService allocationService, ElectMasterService electMaster,
                              Settings settings) {
        super(settings);
        this.masterService = masterService;
        joinTaskExecutor = new JoinTaskExecutor(allocationService, electMaster, logger);
    }

    /**
     * waits for enough incoming joins from master eligible nodes to complete the master election
     * <p>
     * You must start accumulating joins before calling this method. See {@link #startElectionContext()}
     * <p>
     * The method will return once the local node has been elected as master or some failure/timeout has happened.
     * The exact outcome is communicated via the callback parameter, which is guaranteed to be called.
     *
     * @param requiredMasterJoins the number of joins from master eligible needed to complete the election
     * @param timeValue           how long to wait before failing. a timeout is communicated via the callback's onFailure method.
     * @param callback            the result of the election (success or failure) will be communicated by calling methods on this
     *                            object
     **/
    //Michel:等待足够候选主节点票数来完成主节点选举（在调用之前需要先调用tartElectionContext()积累票数）
    public void waitToBeElectedAsMaster(int requiredMasterJoins, TimeValue timeValue, final ElectionCallback callback) {
        final CountDownLatch done = new CountDownLatch(1);//Michel:等待选举节点的栅栏
        //Michel:封装回调（增加对栅栏的操作，以控制等待）
        final ElectionCallback wrapperCallback = new ElectionCallback() {
            @Override
            public void onElectedAsMaster(ClusterState state) {
                done.countDown();
                callback.onElectedAsMaster(state);
            }

            @Override
            public void onFailure(Throwable t) {
                done.countDown();
                callback.onFailure(t);
            }
        };

        ElectionContext myElectionContext = null;

        try {
            // check what we have so far..
            // capture the context we add the callback to make sure we fail our own
            synchronized (this) {
                assert electionContext != null : "waitToBeElectedAsMaster is called we are not accumulating joins";
                myElectionContext = electionContext;
                electionContext.onAttemptToBeElected(requiredMasterJoins, wrapperCallback);
                checkPendingJoinsAndElectIfNeeded();
            }

            try {
                if (done.await(timeValue.millis(), TimeUnit.MILLISECONDS)) {
                    // callback handles everything
                    return;
                }
            } catch (InterruptedException e) {

            }
            if (logger.isTraceEnabled()) {
                final int pendingNodes = myElectionContext.getPendingMasterJoinsCount();
                logger.trace("timed out waiting to be elected. waited [{}]. pending master node joins [{}]", timeValue, pendingNodes);
            }
            failContextIfNeeded(myElectionContext, "timed out waiting to be elected");
        } catch (Exception e) {
            logger.error("unexpected failure while waiting for incoming joins", e);
            if (myElectionContext != null) {
                failContextIfNeeded(myElectionContext, "unexpected failure while waiting for pending joins [" + e.getMessage() + "]");
            }
        }
    }

    /**
     * utility method to fail the given election context under the cluster state thread
     */
    //Michel: 选举过程出现问题，如等待超时或关闭异常时调用
    private synchronized void failContextIfNeeded(final ElectionContext context, final String reason) {
        if (electionContext == context) {
            stopElectionContext(reason);
        }
    }

    /**
     * Accumulates any future incoming join request. Pending join requests will be processed in the final steps of becoming a
     * master or when {@link #stopElectionContext(String)} is called.
     */
    //Michel:初始化一个electionContext用于积累其他节点的加入请求（票数）
    public synchronized void startElectionContext() {
        logger.trace("starting an election context, will accumulate joins");
        assert electionContext == null : "double startElectionContext() calls";
        electionContext = new ElectionContext();
    }

    /**
     * Stopped accumulating joins. All pending joins will be processed. Future joins will be processed immediately
     */
    public void stopElectionContext(String reason) {
        logger.trace("stopping election ([{}])", reason);
        synchronized (this) {
            assert electionContext != null : "stopElectionContext() called but not accumulating";
            electionContext.closeAndProcessPending(reason);
            electionContext = null;
        }
    }

    /**
     * processes or queues an incoming join request.
     * <p>
     * Note: doesn't do any validation. This should have been done before.
     */
    public synchronized void handleJoinRequest(final DiscoveryNode node, final MembershipAction.JoinCallback callback) {
        if (electionContext != null) {
            electionContext.addIncomingJoin(node, callback);
            checkPendingJoinsAndElectIfNeeded();
        } else {
            masterService.submitStateUpdateTask("zen-disco-node-join",
                node, ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new JoinTaskListener(callback, logger));
        }
    }

    /**
     * checks if there is an on going request to become master and if it has enough pending joins. If so, the node will
     * become master via a ClusterState update task.
     */
    //Michel:查看票数是否足够，足够则成为主节点，否则什么都不做
    private synchronized void checkPendingJoinsAndElectIfNeeded() {
        assert electionContext != null : "election check requested but no active context";
        final int pendingMasterJoins = electionContext.getPendingMasterJoinsCount();
        if (electionContext.isEnoughPendingJoins(pendingMasterJoins) == false) {
            if (logger.isTraceEnabled()) {
                logger.trace("not enough joins for election. Got [{}], required [{}]", pendingMasterJoins,
                    electionContext.requiredMasterJoins);
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("have enough joins for election. Got [{}], required [{}]", pendingMasterJoins,
                    electionContext.requiredMasterJoins);
            }
            electionContext.closeAndBecomeMaster();
            electionContext = null; // clear this out so future joins won't be accumulated
        }
    }

    //Michel:选举触发回调动作抽象接口
    public interface ElectionCallback {
        /**
         * called when the local node is successfully elected as master
         * Guaranteed to be called on the cluster state update thread
         **/
        void onElectedAsMaster(ClusterState state);

        /**
         * called when the local node failed to be elected as master
         * Guaranteed to be called on the cluster state update thread
         **/
        void onFailure(Throwable t);
    }

    //Michel:主节点选举上下文
    class ElectionContext {
        private ElectionCallback callback = null;
        private int requiredMasterJoins = -1;//Michel:需要加入的主节点个数,需要至少这么多节点认可才可以成为主节点
        private final Map<DiscoveryNode, List<MembershipAction.JoinCallback>> joinRequestAccumulator = new HashMap<>();

        final AtomicBoolean closed = new AtomicBoolean();//Michel:关闭开关

        //Michel:在开始尝试被选举为主节点时：需要的最小票数+选举成功回调
        public synchronized void onAttemptToBeElected(int requiredMasterJoins, ElectionCallback callback) {
            ensureOpen();
            assert this.requiredMasterJoins < 0;
            assert this.callback == null;
            this.requiredMasterJoins = requiredMasterJoins;
            this.callback = callback;
        }

        //Michel:当有加入请求时，将请求加入列表中
        public synchronized void addIncomingJoin(DiscoveryNode node, MembershipAction.JoinCallback callback) {
            ensureOpen();
            joinRequestAccumulator.computeIfAbsent(node, n -> new ArrayList<>()).add(callback);
        }

        //Michel:判断当前票数是否足够
        public synchronized boolean isEnoughPendingJoins(int pendingMasterJoins) {
            final boolean hasEnough;
            if (requiredMasterJoins < 0) {
                // requiredMasterNodes is unknown yet, return false and keep on waiting
                hasEnough = false;
            } else {
                assert callback != null : "requiredMasterJoins is set but not the callback";
                hasEnough = pendingMasterJoins >= requiredMasterJoins;
            }
            return hasEnough;
        }

        //Michel:获取积累的请求
        private Map<DiscoveryNode, ClusterStateTaskListener> getPendingAsTasks() {
            Map<DiscoveryNode, ClusterStateTaskListener> tasks = new HashMap<>();
            joinRequestAccumulator.entrySet().stream().forEach(e -> tasks.put(e.getKey(), new JoinTaskListener(e.getValue(), logger)));
            return tasks;
        }

        //Michel:获取当前票数(只统计候选主节点)
        public synchronized int getPendingMasterJoinsCount() {
            int pendingMasterJoins = 0;
            for (DiscoveryNode node : joinRequestAccumulator.keySet()) {
                if (node.isMasterNode()) {
                    pendingMasterJoins++;
                }
            }
            return pendingMasterJoins;
        }
        //Michel:当票数足够后，关闭context，并成为主节点
        public synchronized void closeAndBecomeMaster() {
            assert callback != null : "becoming a master but the callback is not yet set";
            assert isEnoughPendingJoins(getPendingMasterJoinsCount()) : "becoming a master but pending joins of "
                + getPendingMasterJoinsCount() + " are not enough. needs [" + requiredMasterJoins + "];";

            innerClose();

            Map<DiscoveryNode, ClusterStateTaskListener> tasks = getPendingAsTasks();
            final String source = "zen-disco-elected-as-master ([" + tasks.size() + "] nodes joined)";

            tasks.put(BECOME_MASTER_TASK, (source1, e) -> {
            }); // noop listener, the election finished listener determines result
            tasks.put(FINISH_ELECTION_TASK, electionFinishedListener);
            masterService.submitStateUpdateTasks(source, tasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
        }

        public synchronized void closeAndProcessPending(String reason) {
            innerClose();
            Map<DiscoveryNode, ClusterStateTaskListener> tasks = getPendingAsTasks();
            final String source = "zen-disco-election-stop [" + reason + "]";
            tasks.put(FINISH_ELECTION_TASK, electionFinishedListener);
            masterService.submitStateUpdateTasks(source, tasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
        }
        //Michel:关闭context
        private void innerClose() {
            if (closed.getAndSet(true)) {
                throw new AlreadyClosedException("election context is already closed");
            }
        }
        //Michel:确保当前context处于开启状态
        private void ensureOpen() {
            if (closed.get()) {
                throw new AlreadyClosedException("election context is already closed");
            }
        }
        //Michel:获取选举回调
        private synchronized ElectionCallback getCallback() {
            return callback;
        }

        private void onElectedAsMaster(ClusterState state) {
            assert MasterService.assertMasterUpdateThread();
            assert state.nodes().isLocalNodeElectedMaster() : "onElectedAsMaster called but local node is not master";
            ElectionCallback callback = getCallback(); // get under lock
            if (callback != null) {
                callback.onElectedAsMaster(state);
            }
        }

        private void onFailure(Throwable t) {
            assert MasterService.assertMasterUpdateThread();
            ElectionCallback callback = getCallback(); // get under lock
            if (callback != null) {
                callback.onFailure(t);
            }
        }
        //Michel:选举结束监听器（分选为主节点，未被选为主节点和异常三种情况）
        private final ClusterStateTaskListener electionFinishedListener = new ClusterStateTaskListener() {

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (newState.nodes().isLocalNodeElectedMaster()) {
                    ElectionContext.this.onElectedAsMaster(newState);
                } else {
                    onFailure(source, new NotMasterException("election stopped [" + source + "]"));
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                //Grammar:非静态内部类访问外部类的成员方法要使用OuterClass.this.fun()
                ElectionContext.this.onFailure(e);
            }
        };

    }

    //Michel:加入集群监听器：封装加入集群回调，并记录日志（主要就是onFailure和clusterSateProcessed，批量处理tasks）
    static class JoinTaskListener implements ClusterStateTaskListener {
        final List<MembershipAction.JoinCallback> callbacks;
        private final Logger logger;

        JoinTaskListener(MembershipAction.JoinCallback callback, Logger logger) {
            this(Collections.singletonList(callback), logger);
        }

        JoinTaskListener(List<MembershipAction.JoinCallback> callbacks, Logger logger) {
            this.callbacks = callbacks;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            for (MembershipAction.JoinCallback callback : callbacks) {
                try {
                    callback.onFailure(e);
                } catch (Exception inner) {
                    logger.error(() -> new ParameterizedMessage("error handling task failure [{}]", e), inner);
                }
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            for (MembershipAction.JoinCallback callback : callbacks) {
                try {
                    callback.onSuccess();
                } catch (Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected error during [{}]", source), e);
                }
            }
        }
    }

    /**
     * a task indicated that the current node should become master, if no current master is known
     */
    public static final DiscoveryNode BECOME_MASTER_TASK = new DiscoveryNode("_BECOME_MASTER_TASK_",
        new TransportAddress(TransportAddress.META_ADDRESS, 0),
        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT) {
        @Override
        public String toString() {
            return ""; // this is not really task , so don't log anything about it...
        }
    };

    /**
     * a task that is used to signal the election is stopped and we should process pending joins.
     * it may be use in combination with {@link #BECOME_MASTER_TASK}
     */
    public static final DiscoveryNode FINISH_ELECTION_TASK = new DiscoveryNode("_FINISH_ELECTION_",
        new TransportAddress(TransportAddress.META_ADDRESS, 0), Collections.emptyMap(), Collections.emptySet(), Version.CURRENT) {
        @Override
        public String toString() {
            return ""; // this is not really task , so don't log anything about it...
        }
    };

    // visible for testing
    public static class JoinTaskExecutor implements ClusterStateTaskExecutor<DiscoveryNode> {

        private final AllocationService allocationService;

        private final ElectMasterService electMasterService;

        private final Logger logger;

        public JoinTaskExecutor(AllocationService allocationService, ElectMasterService electMasterService, Logger logger) {
            this.allocationService = allocationService;
            this.electMasterService = electMasterService;
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<DiscoveryNode> execute(ClusterState currentState, List<DiscoveryNode> joiningNodes) throws Exception {
            final ClusterTasksResult.Builder<DiscoveryNode> results = ClusterTasksResult.builder();

            final DiscoveryNodes currentNodes = currentState.nodes();
            boolean nodesChanged = false;
            ClusterState.Builder newState;

            if (joiningNodes.size() == 1 && joiningNodes.get(0).equals(FINISH_ELECTION_TASK)) {
                return results.successes(joiningNodes).build(currentState);
            } else if (currentNodes.getMasterNode() == null && joiningNodes.contains(BECOME_MASTER_TASK)) {
                assert joiningNodes.contains(FINISH_ELECTION_TASK) : "becoming a master but election is not finished " + joiningNodes;
                // use these joins to try and become the master.
                // Note that we don't have to do any validation of the amount of joining nodes - the commit
                // during the cluster state publishing guarantees that we have enough
                newState = becomeMasterAndTrimConflictingNodes(currentState, joiningNodes);
                nodesChanged = true;
            } else if (currentNodes.isLocalNodeElectedMaster() == false) {
                logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
                throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
            } else {
                newState = ClusterState.builder(currentState);
            }

            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

            assert nodesBuilder.isLocalNodeElectedMaster();

            Version minClusterNodeVersion = newState.nodes().getMinNodeVersion();
            Version maxClusterNodeVersion = newState.nodes().getMaxNodeVersion();
            // we only enforce major version transitions on a fully formed clusters
            final boolean enforceMajorVersion = currentState.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
            // processing any joins
            for (final DiscoveryNode node : joiningNodes) {
                if (node.equals(BECOME_MASTER_TASK) || node.equals(FINISH_ELECTION_TASK)) {
                    // noop
                } else if (currentNodes.nodeExists(node)) {
                    logger.debug("received a join request for an existing node [{}]", node);
                } else {
                    try {
                        if (enforceMajorVersion) {
                            MembershipAction.ensureMajorVersionBarrier(node.getVersion(), minClusterNodeVersion);
                        }
                        MembershipAction.ensureNodesCompatibility(node.getVersion(), minClusterNodeVersion, maxClusterNodeVersion);
                        // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                        // we have to reject nodes that don't support all indices we have in this cluster
                        MembershipAction.ensureIndexCompatibility(node.getVersion(), currentState.getMetaData());
                        nodesBuilder.add(node);
                        nodesChanged = true;
                        minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                        maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                    } catch (IllegalArgumentException | IllegalStateException e) {
                        results.failure(node, e);
                        continue;
                    }
                }
                results.success(node);
            }
            if (nodesChanged) {
                newState.nodes(nodesBuilder);
                return results.build(allocationService.reroute(newState.build(), "node_join"));
            } else {
                // we must return a new cluster state instance to force publishing. This is important
                // for the joining node to finalize its join and set us as a master
                return results.build(newState.build());
            }
        }

        private ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<DiscoveryNode> joiningNodes) {
            assert currentState.nodes().getMasterNodeId() == null : currentState;
            DiscoveryNodes currentNodes = currentState.nodes();
            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
            nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());

            for (final DiscoveryNode joiningNode : joiningNodes) {
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                }
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameAddress,
                        joiningNode);
                    nodesBuilder.remove(nodeWithSameAddress.getId());
                }
            }


            // now trim any left over dead nodes - either left there when the previous master stepped down
            // or removed by us above
            ClusterState tmpState = ClusterState.builder(currentState).nodes(nodesBuilder).blocks(ClusterBlocks.builder()
                .blocks(currentState.blocks())
                .removeGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID)).build();
            return ClusterState.builder(allocationService.deassociateDeadNodes(tmpState, false,
                "removed dead nodes on election"));
        }

        @Override
        public boolean runOnlyOnMaster() {
            // we validate that we are allowed to change the cluster state during cluster state processing
            return false;
        }

        @Override
        public void clusterStatePublished(ClusterChangedEvent event) {
            electMasterService.logMinimumMasterNodesWarningIfNecessary(event.previousState(), event.state());
        }
    }
}
