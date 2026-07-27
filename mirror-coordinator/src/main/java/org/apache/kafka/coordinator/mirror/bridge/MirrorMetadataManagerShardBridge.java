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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.mirror.MirrorPartition;
import org.apache.kafka.coordinator.mirror.MirrorPartitionKey;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Optional;

/**
 * Bridge between the {@link org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorShard}
 * (mirror-coordinator module) and {@code MirrorMetadataManager} / {@code MetadataCache} (core module).
 */
public interface MirrorMetadataManagerShardBridge {
    /** Called after the shard finishes loading and transitions to ACTIVE. */
    void onShardLoaded();

    /** Called when the shard is unloaded. Clears cache entries whose keys hash to the given partition index. */
    void onShardUnloaded(int partitionIndex, int numPartitions);

    /** Sets the last mirror epoch for a single partition. */
    void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch);

    /** Returns the current partition state, or {@code UNKNOWN} if absent. */
    MirrorPartitionState getPartitionState(String mirrorName, TopicPartition topicPartition);

    /** Sets the partition state in the in-memory cache (called during replay). */
    void setPartitionState(MirrorPartitionKey key, MirrorPartitionState newState);

    /** Clears a partition entry from the in-memory cache (tombstone replay). */
    void clearPartitionState(MirrorPartitionKey key);

    /** Gets the mirror partition metadata, or {@code null} if not tracked. */
    MirrorPartition getFailedInfo(MirrorPartitionKey key);

    /** Sets error info (retry attempt, error, previous state). */
    void setFailedInfo(MirrorPartitionKey key, MirrorPartition info);

    /** Clears error info when a partition leaves the FAILED state. */
    void clearFailedInfo(MirrorPartitionKey key);

    /** Updates failed partition info for a state transition. */
    void updateFailedInfo(MirrorPartitionKey key, MirrorPartitionState currentState,
                          MirrorPartitionState newState, String errorMessage, boolean nonRetryable);

    /** Resolves a topic name to its ID via the metadata cache. */
    Uuid getTopicId(String topicName);

    /** Resolves a topic ID to its name via the metadata cache. */
    Optional<String> getTopicName(Uuid topicId);
}
