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

import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteClusterMirrorRequestData;
import org.apache.kafka.common.message.PauseMirrorTopicsRequestData;
import org.apache.kafka.common.message.ResumeMirrorTopicsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorShard;
import org.apache.kafka.coordinator.mirror.ClusterMirrorPartitionKey;
import org.apache.kafka.coordinator.mirror.PartitionStateInfo;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Bridge between the {@link org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorService}
 * (mirror-coordinator module) and {@code MirrorMetadataManager} / {@code MetadataCache} (core module).
 */
public interface MirrorMetadataManagerServiceBridge {
    /** Sentinel value indicating a non-retryable failed partition. */
    int NON_RETRYABLE_ATTEMPT = -1;

    // -- Lifecycle --

    /**
     * Initializes the metadata manager with callbacks from the coordinator service.
     * Called once during {@code ClusterMirrorCoordinatorService.start()}.
     *
     * @param stateTransitioner       callback to transition partition states
     * @param tombstoneWriter         callback to write tombstone records for a deleted mirror
     * @param coordPartitionByKeyFinder maps a partition key to a {@code __mirror_state} partition index
     * @param coordPartitionByNameFinder maps a mirror name to a {@code __mirror_state} partition index
     */
    void initialize(
        StateTransitioner stateTransitioner,
        Consumer<String> tombstoneWriter,
        Function<ClusterMirrorPartitionKey, Integer> coordPartitionByKeyFinder,
        Function<String, Integer> coordPartitionByNameFinder
    );

    /** Closes all source cluster admin clients immediately. */
    void closeSourceAdmins();

    // -- State cache --

    /** Returns the cached partition state, or {@code null} if not tracked. */
    MirrorPartitionState getPartitionState(String mirrorName, TopicPartition tp);

    /** Updates the cached partition state for the given key. */
    void setPartitionState(ClusterMirrorPartitionKey key, MirrorPartitionState state);

    /** Removes the cached partition state for the given key. */
    void clearPartitionState(ClusterMirrorPartitionKey key);

    /** Updates failure tracking metadata for a state transition. */
    void updateFailedState(
        ClusterMirrorPartitionKey key,
        MirrorPartitionState currentState,
        MirrorPartitionState newState,
        String errorMessage,
        boolean nonRetryable
    );

    /** Returns failure tracking metadata for the given key, or {@code null}. */
    ClusterMirrorCoordinatorShard.FailedPartitionInfo getFailedInfo(ClusterMirrorPartitionKey key);

    /** Sets the last mirror epoch in the local cache. */
    void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch);

    // -- Network/RPC --

    /** Writes partition states to remote coordinator brokers via batched RPCs. */
    void writeStatesToRemoteCoordinator(
        String mirrorName,
        Map<String, Set<PartitionStateInfo>> topicMetadata,
        Set<String> stoppedTopics,
        Consumer<WriteMirrorStatesResponse> callback
    );

    /** Reads partition states from remote coordinator brokers via batched RPCs. */
    void readMirrorStates(
        String mirrorName,
        Map<String, Set<Integer>> partitions,
        Consumer<ReadMirrorStatesResponse> callback
    );

    /** Sends a last-mirror-epoch lookup to the source cluster. */
    CompletionStage<Map<TopicPartition, Integer>> sendLastMirrorEpochLookup(
        String mirrorName,
        TopicPartition tp,
        Collection<ClusterMirrorListing> sourceMirrors
    );

    /** Requests a leader epoch bump for the given partitions via the controller. */
    CompletableFuture<Void> bumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs);

    /** Schedules a leader epoch bump for a single mirror partition. */
    CompletableFuture<Void> scheduleBumpLeaderEpoch(String mirrorName, TopicPartition tp);

    // -- Source syncer --

    /** Returns the list of cluster mirrors configured on the source cluster. */
    Collection<ClusterMirrorListing> listSourceClusterMirrors(String mirrorName);

    /** Checks whether mirroring the given partition would create a loop. */
    boolean hasMirrorLoop(String mirrorName, TopicPartition tp, Collection<ClusterMirrorListing> sourceMirrors);

    // -- Query --

    /** Returns the source cluster ID from the mirror config, or {@code null}. */
    String getSourceClusterId(String mirrorName);

    /** Returns the source bootstrap servers from the mirror config, or {@code null}. */
    String getSourceBootstrap(String mirrorName);

    /** Returns all mirror names present in the metadata image. */
    Set<String> getConfiguredMirrors();

    /** Returns cached partition states for all partitions of the given mirror. */
    Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName);

    /** Returns topic names configured for the given mirror, with optional state filters. */
    Set<String> getConfiguredTopics(String mirrorName, boolean includePaused, boolean includeStopped);

    /** Returns the number of actively mirrored topics for the given mirror. */
    int getActiveTopicCount(String mirrorName);

    // -- Cache cleanup --

    /** Removes all cached state for the given mirror. */
    void removeCachedMirror(String mirrorName);

    /** Removes pending epoch bump state for the given partitions. */
    void removeStateForPartitions(Set<TopicPartition> partitions);

    // -- Validation --

    /** Validates partition states before a start-mirror-topics request. */
    void validateStartMirrorStates(StartMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback);

    /** Validates partition states before a stop-mirror-topics request. */
    void validateStopMirrorStates(StopMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback);

    /** Validates partition states before a pause-mirror-topics request. */
    void validatePauseMirrorStates(PauseMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback);

    /** Validates partition states before a resume-mirror-topics request. */
    void validateResumeMirrorStates(ResumeMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback);

    /** Validates partition states before a delete-cluster-mirror request. */
    void validateDeleteMirrorStates(DeleteClusterMirrorRequestData data, Consumer<Optional<Errors>> callback);

    // -- LME --

    /** Processes a last-mirror-epoch lookup from a remote broker. */
    Map<String, Map<TopicPartition, Integer>> processLastMirrorEpochLookup(
        Map<String, Map<String, Set<Integer>>> mirrorPartitions
    );

    // -- Metadata scheduling --

    /** Schedules periodic metadata refresh from source clusters. */
    void scheduleMetadataRefresh(long intervalMs);

    // -- Topic resolution --

    /** Resolves a topic name to its topic ID from the metadata cache. */
    Uuid getTopicId(String topicName);

    /** Resolves a topic ID to its name from the metadata cache. */
    Optional<String> getTopicName(Uuid topicId);

    // -- Leader check --

    /**
     * Returns the leader node ID for the given topic partition, or {@code -1}
     * if the leader is unknown.
     */
    int getLeaderForPartition(String topic, int partition);

    // -- State transition callback --

    /** Callback for partition state transitions triggered by the coordinator. */
    interface StateTransitioner {
        /** Transitions the given partitions to the specified state. */
        void transitionTo(
            String mirrorName,
            Set<TopicPartition> topicPartition,
            MirrorPartitionState state,
            String errorMessage,
            boolean nonRetryable
        );
    }
}
