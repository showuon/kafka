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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Bridge between the mirror-coordinator module and core module classes
 * ({@code MirrorMetadataManager}, {@code MetadataCache}, {@code ReplicaManager}).
 */
public interface CoreBridge {
    void initialize(
        CoordinatorWriter coordinatorWriter,
        Function<MirrorPartitionKey, Integer> coordPartFinderByKey,
        Function<String, Integer> coordPartFinderByName
    );

    void closeSourceAdmins();

    void onShardLoaded();

    void onShardUnloaded(int coordPartition, int coordPartitionCount);

    MirrorPartition getPartition(MirrorPartitionKey key);

    void setPartition(MirrorPartitionKey key, MirrorPartition partition);

    void removePartition(MirrorPartitionKey key);

    void updateFailedInfo(
        MirrorPartitionKey key,
        MirrorPartitionState curState,
        MirrorPartitionState newState,
        String errorMessage,
        boolean nonRetryable
    );

    void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch);

    Uuid getTopicId(String topicName);

    Optional<String> getTopicName(Uuid topicId);

    /**
     * Callback for writing coordinator records to the {@code __mirror_state} shard
     * via the {@code CoordinatorRuntime}. Implemented by
     * {@code ClusterMirrorCoordinatorService} and passed to
     * {@code MirrorMetadataManager} during initialization.
     */
    interface CoordinatorWriter {
        /** Persists a partition state transition record to the coordinator shard. */
        CompletableFuture<Void> writeTransition(
            String mirrorName,
            TopicPartition tp,
            MirrorPartitionState state,
            String errorMessage,
            boolean nonRetryable
        );

        /** Persists a last mirror epoch record to the coordinator shard. */
        CompletableFuture<Void> writeLastMirrorEpoch(
            String mirrorName,
            TopicPartition tp,
            int epoch
        );

        /** Persists tombstone records for the given partitions of a deleted mirror. */
        CompletableFuture<Void> writeTombstone(
            String mirrorName,
            Set<TopicPartition> partitions
        );
    }
}
