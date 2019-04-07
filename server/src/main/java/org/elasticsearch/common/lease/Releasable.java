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

package org.elasticsearch.common.lease;

import org.elasticsearch.ElasticsearchException;

import java.io.Closeable;

/**
 * Specialization of {@link AutoCloseable} that may only throw an {@link ElasticsearchException}.
 */
//Michel：重写Closeable接口，将其中的IOException移出（实现这样的接口是为了配合java的try-with-resource实现资源的自动清理）
//Done
public interface Releasable extends Closeable {

    @Override
    void close();

}
