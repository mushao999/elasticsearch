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

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * A listener interface that allows to react on transport events. All methods may be
 * executed on network threads. Consumers must fork in the case of long running or blocking
 * operations.
 */
//Michel: transport事件监听器的抽象接口定义，当发生对应时间时会调用定义的各方法，可以部分实现
public interface TransportConnectionListener {

    /**
     * Called once a connection was opened
     * @param connection the connection
     */
    default void onConnectionOpened(Transport.Connection connection) {}

    /**
     * Called once a connection ws closed.
     * @param connection the closed connection
     */
    default void onConnectionClosed(Transport.Connection connection) {}

    /**
     * Called once a node connection is opened and registered.
     */
    default void onNodeConnected(DiscoveryNode node) {}

    /**
     * Called once a node connection is closed and unregistered.
     */
    default void onNodeDisconnected(DiscoveryNode node) {}
}
