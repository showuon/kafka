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
package org.apache.kafka.coordinator.mirror.bridge;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Bridge between the {@link org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorService}
 * (mirror-coordinator module) and {@code ReplicaManager} (core module).
 */
public interface ReplicaManagerBridge {
    /**
     * Creates mirror fetcher threads for the given partitions if they do not already exist.
     *
     * @param mirrorName the name of the cluster mirror
     * @param partitions the set of topic partitions to start fetching
     */
    void maybeCreateMirrorFetchers(String mirrorName, Set<TopicPartition> partitions);

    /**
     * Removes mirror fetcher threads for the given partitions.
     *
     * @param partitions the set of topic partitions to stop fetching
     */
    void removeFetcherForPartitions(Set<TopicPartition> partitions);

    /**
     * Returns the latest leader epoch for the given partition's local log.
     *
     * @param tp the topic partition
     * @return the latest epoch, or empty if the log is not available
     */
    OptionalInt getLatestEpoch(TopicPartition tp);

    /**
     * Returns a singleton map from the partition to its latest local leader epoch.
     * Used as input to {@link #maybeTruncateForLeaderEpoch} and
     * {@link org.apache.kafka.coordinator.mirror.bridge.MirrorMetadataManagerServiceBridge#bumpLeaderEpochs}.
     *
     * @param tp the topic partition
     * @return map of partition to epoch
     */
    Map<TopicPartition, Integer> getLatestLocalEpoch(TopicPartition tp);

    /**
     * Truncates the local log to the given leader epochs if needed.
     *
     * @param epochs   map of partition to target epoch
     * @param callback invoked for each partition after truncation completes
     */
    void maybeTruncateForLeaderEpoch(Map<TopicPartition, Integer> epochs, Consumer<TopicPartition> callback);

    /**
     * Aborts all ongoing transactions on the given partition by appending
     * end-transaction markers.
     *
     * @param tp the topic partition
     * @return a future that completes when all abort markers have been appended
     */
    CompletableFuture<Void> abortOngoingTransactions(TopicPartition tp);

    /**
     * Appends a PID reset barrier control record to the given partition.
     * The caller is responsible for retry scheduling on failure.
     *
     * @param tp              the topic partition
     * @param sourceClusterId the source cluster ID written into the barrier record
     * @param timestampMs     the timestamp for the record batch
     * @return a future that completes when the record has been appended
     */
    CompletableFuture<Void> appendPidResetBarrier(TopicPartition tp, String sourceClusterId, long timestampMs);
}
