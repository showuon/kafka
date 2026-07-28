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

import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Bridge between the mirror-coordinator module and core module classes
 * ({@code MirrorMetadataManager}, {@code MetadataCache}, {@code ReplicaManager}).
 */
public interface CoreBridge {
    void initialize(
        StateTransitioner stateTransitioner,
        Consumer<String> tombstoneWriter,
        Function<MirrorPartitionKey, Integer> coordPartitionByKeyFinder,
        Function<String, Integer> coordPartitionByNameFinder
    );

    void closeSourceAdmins();

    void onShardLoaded();

    void onShardUnloaded(int coordPartition, int coordPartitionCount);

    MirrorPartitionState getPartitionState(String mirrorName, TopicPartition tp);

    void setPartitionState(MirrorPartitionKey key, MirrorPartitionState state);

    void removePartitionState(MirrorPartitionKey key);

    MirrorPartition getFailedInfo(MirrorPartitionKey key);

    void setFailedInfo(MirrorPartitionKey key, MirrorPartition info);

    void clearFailedInfo(MirrorPartitionKey key);

    void updateFailedInfo(
        MirrorPartitionKey key,
        MirrorPartitionState currentState,
        MirrorPartitionState newState,
        String errorMessage,
        boolean nonRetryable
    );

    void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch);

    void writeStatesToRemoteCoordinator(
        String mirrorName,
        Map<String, Set<ClusterMirrorCoordinatorService.MirrorStateWrite>> topicMetadata,
        Set<String> stoppedTopics,
        Consumer<WriteMirrorStatesResponse> callback
    );

    void readStatesFromLocalCoordinator(
        String mirrorName,
        Map<String, Set<Integer>> partitions,
        Consumer<ReadMirrorStatesResponse> callback
    );

    CompletionStage<Map<TopicPartition, Integer>> sendLastMirrorEpochLookup(
        String mirrorName,
        TopicPartition tp,
        Collection<ClusterMirrorListing> sourceMirrors
    );

    CompletableFuture<Void> bumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs);

    CompletableFuture<Void> scheduleBumpLeaderEpoch(String mirrorName, TopicPartition tp);

    Collection<ClusterMirrorListing> listSourceClusterMirrors(String mirrorName);

    boolean hasMirrorLoop(String mirrorName, TopicPartition tp, Collection<ClusterMirrorListing> sourceMirrors);

    String getSourceClusterId(String mirrorName);

    Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName);

    void removeMirror(String mirrorName);

    void removePendingEpochBumps(Set<TopicPartition> partitions);

    Uuid getTopicId(String topicName);

    Optional<String> getTopicName(Uuid topicId);

    int getLeaderForPartition(String topic, int partition);

    void maybeCreateMirrorFetchers(String mirrorName, Set<TopicPartition> partitions);

    void removeFetcherForPartitions(Set<TopicPartition> partitions);

    OptionalInt getLatestEpoch(TopicPartition tp);

    Map<TopicPartition, Integer> getLatestLocalEpoch(TopicPartition tp);

    void maybeTruncateForLeaderEpoch(Map<TopicPartition, Integer> epochs, Consumer<TopicPartition> callback);

    CompletableFuture<Void> abortOngoingTransactions(TopicPartition tp);

    CompletableFuture<Void> appendPidResetBarrier(TopicPartition tp, String sourceClusterId, long timestampMs);

    interface StateTransitioner {
        void transitionTo(
            String mirrorName,
            Set<TopicPartition> topicPartition,
            MirrorPartitionState state,
            String errorMessage,
            boolean nonRetryable
        );
    }
}
