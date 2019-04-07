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

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.Writeable;


/**
 * Cluster state part, changes in which can be serialized
 */
//Michel: 表示一个可以比较不同的对象（看注释主要用于设定集群状态是可比较不同的）
public interface Diffable<T> extends Writeable {

    /**
     * Returns serializable object representing differences between this and previousState
     */
    //Michel:与previousState对象比较不同
    Diff<T> diff(T previousState);

}
