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

import org.apache.kafka.common.EpochOffset;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.server.mirror.MirrorPartitionKey;
import org.apache.kafka.server.mirror.MirrorPartitionMetadata;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Bridge between the coordinator (mirror-coordinator module) and metadata manager (core module).
 * This interface defines the contract for state persistence and retrieval.
 */
public interface MetadataManagerBridge {
    void onBrokerStart(
        Function<MirrorPartitionKey, Integer> coordPartFinder,
        CoordinatorReader coordinatorReader,
        CoordinatorWriter coordinatorWriter
    );

    void onShardLoaded(int coordPartition);

    void onShardUnloaded(int coordPartition, int coordPartitionCount);

    void closeSourceAdmins();

    Uuid getTopicId(String topicName);

    Optional<String> getTopicName(Uuid topicId);

    MirrorPartitionMetadata getPartitionMetadata(MirrorPartitionKey key);

    void setPartitionMetadata(MirrorPartitionKey key, MirrorPartitionMetadata partition);

    void removePartitionMetadata(MirrorPartitionKey key);

    void updateFailureDetails(
        MirrorPartitionKey key,
        MirrorPartitionState curState,
        MirrorPartitionState newState,
        String errorMessage,
        boolean nonRetryable
    );

    void setLastMirrorPosition(String mirrorName, String topic, int partition, EpochOffset lastMirrorPosition);

    /**
     * Callback for reading partition state from the {@code __mirror_state} shard
     * via the {@code CoordinatorRuntime}. Delegating the read through the runtime
     * (rather than reading MMM's local cache directly) ensures that a shard still
     * loading surfaces as {@code COORDINATOR_LOAD_IN_PROGRESS} instead of returning
     * possibly-incomplete cached state.
     */
    @FunctionalInterface
    interface CoordinatorReader {
        CompletableFuture<ReadMirrorStatesResponseData> readPartitionStates(
                String mirrorName,
                Map<String, Set<Integer>> partitions
        );
    }

    /**
     * Callback for writing coordinator records to the {@code __mirror_state} shard
     * via the {@code CoordinatorRuntime}. This is the seam between the two modules:
     * MMM (core) decides what to write; the coordinator service (mirror-coordinator)
     * knows how to write it.
     */
    interface CoordinatorWriter {
        CompletableFuture<WriteMirrorStatesResponseData> writePartitionStates(
            String mirrorName,
            Map<String, Set<ClusterMirrorCoordinatorService.MirrorStateWrite>> states
        );

        CompletableFuture<Void> writeLastMirrorPositions(
            String mirrorName,
            Map<TopicPartition, EpochOffset> positions
        );

        CompletableFuture<Void> writeMirrorTombstones(
            String mirrorName,
            Set<TopicPartition> partitions
        );
    }
}
