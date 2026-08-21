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
import org.apache.kafka.common.TopicPartition;

import java.util.Set;

/**
 * The result of the {@link Admin#recoverMirrorTopics(String, Set, RecoverMirrorTopicsOptions)} call.
 */
public class RecoverMirrorTopicsResult {
    private final KafkaFuture<Set<TopicPartition>> recoveredPartitionsFuture;

    RecoverMirrorTopicsResult(final KafkaFuture<Set<TopicPartition>> recoveredPartitionsFuture) {
        this.recoveredPartitionsFuture = recoveredPartitionsFuture;
    }

    public KafkaFuture<Void> all() {
        return recoveredPartitionsFuture.thenApply(partitions -> null);
    }

    /**
     * Returns the set of partitions that were found in a FAILED state and had recovery triggered.
     * Partitions that were already healthy are not included.
     */
    public KafkaFuture<Set<TopicPartition>> recoveredPartitions() {
        return recoveredPartitionsFuture;
    }
}
