/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.OffsetEpoch;

import java.util.Collections;
import java.util.Map;

/**
 * The result of the {@link Admin#describeClusterMirrors} call.
 */
public class DescribeClusterMirrorsResult {
    private final KafkaFuture<Map<String, ClusterMirrorDescription>> future;
    private final KafkaFuture<Map<String, Map<Integer, OffsetEpoch>>> lastMirrorFuture;

    DescribeClusterMirrorsResult(KafkaFuture<Map<String, ClusterMirrorDescription>> future) {
        this(future, KafkaFuture.completedFuture(Collections.emptyMap()));
    }

    DescribeClusterMirrorsResult(KafkaFuture<Map<String, ClusterMirrorDescription>> future,
                                 KafkaFuture<Map<String, Map<Integer, OffsetEpoch>>> lastMirrorFuture) {
        this.future = future;
        this.lastMirrorFuture = lastMirrorFuture;
    }

    /**
     * Return a future which succeeds only if all the mirror descriptions succeed.
     */
    public KafkaFuture<Map<String, ClusterMirrorDescription>> allDescriptions() {
        return future;
    }

    /**
     * Return a future containing last mirror epoch and offset lookup results.
     * Keyed by topicName, then partitionIndex to {@link OffsetEpoch}.
     */
    public KafkaFuture<Map<String, Map<Integer, OffsetEpoch>>> lastMirrors() {
        return lastMirrorFuture;
    }
}
