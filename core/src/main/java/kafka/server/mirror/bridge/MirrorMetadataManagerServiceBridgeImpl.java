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
package kafka.server.mirror.bridge;

import kafka.server.mirror.MirrorMetadataManager;
import kafka.server.mirror.MirrorMetadataManager.FailedPartitionInfo;

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
import org.apache.kafka.coordinator.mirror.bridge.MirrorMetadataManagerServiceBridge;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public class MirrorMetadataManagerServiceBridgeImpl implements MirrorMetadataManagerServiceBridge {
    private final MirrorMetadataManager metadataManager;
    private final MetadataCache metadataCache;

    public MirrorMetadataManagerServiceBridgeImpl(MirrorMetadataManager metadataManager, MetadataCache metadataCache) {
        this.metadataManager = metadataManager;
        this.metadataCache = metadataCache;
    }

    // Lifecycle

    @Override
    public void initialize(
        StateTransitioner stateTransitioner,
        LastMirrorEpochUpdater lastMirrorEpochUpdater,
        Consumer<String> tombstoneWriter,
        Function<ClusterMirrorPartitionKey, Integer> coordPartitionByKeyFinder,
        Function<String, Integer> coordPartitionByNameFinder
    ) {
        metadataManager.initialize(
            (mn, tps, st, em, nr) -> stateTransitioner.transitionTo(mn, tps, st, em, nr),
            (mn, tp, epoch) -> lastMirrorEpochUpdater.update(mn, tp, epoch),
            tombstoneWriter,
            coordPartitionByKeyFinder,
            coordPartitionByNameFinder);
    }

    @Override
    public void closeSourceAdmins() {
        metadataManager.closeSourceAdmins();
    }

    @Override
    public void handleSideEffect(String mirrorName, TopicPartition tp, MirrorPartitionState newState) {
        metadataManager.handleSideEffect(mirrorName, tp, newState);
    }

    // State cache

    @Override
    public MirrorPartitionState getPartitionState(String mirrorName, TopicPartition tp) {
        return metadataManager.getPartitionState(mirrorName, tp);
    }

    @Override
    public void setPartitionState(ClusterMirrorPartitionKey key, MirrorPartitionState state) {
        metadataManager.setPartitionState(key, state);
    }

    @Override
    public void clearPartitionState(ClusterMirrorPartitionKey key) {
        metadataManager.clearPartitionState(key);
    }

    @Override
    public void updateFailedState(
        ClusterMirrorPartitionKey key,
        MirrorPartitionState currentState,
        MirrorPartitionState newState,
        String errorMessage,
        boolean nonRetryable
    ) {
        metadataManager.updateFailedState(key, currentState, newState, errorMessage, nonRetryable);
    }

    @Override
    public ClusterMirrorCoordinatorShard.FailedPartitionInfo getFailedInfo(ClusterMirrorPartitionKey key) {
        FailedPartitionInfo fpi = metadataManager.getFailedInfo(key);
        if (fpi == null) return null;
        return new ClusterMirrorCoordinatorShard.FailedPartitionInfo(
            fpi.retryAttempt(), fpi.errorMessage(), fpi.previousState());
    }

    @Override
    public void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch) {
        metadataManager.setLastMirrorEpoch(mirrorName, topic, partition, epoch);
    }

    // Network/RPC

    @Override
    public void writeStatesToRemoteCoordinator(
        String mirrorName,
        Map<String, Set<PartitionStateInfo>> topicMetadata,
        Set<String> stoppedTopics,
        Consumer<WriteMirrorStatesResponse> callback
    ) {
        metadataManager.writeStatesToRemoteCoordinator(mirrorName, topicMetadata, stoppedTopics, callback);
    }

    @Override
    public void readMirrorStates(
        String mirrorName,
        Map<String, Set<Integer>> partitions,
        Consumer<ReadMirrorStatesResponse> callback
    ) {
        metadataManager.readMirrorStates(mirrorName, partitions, callback);
    }

    // Query

    @Override
    public String getSourceClusterId(String mirrorName) {
        return metadataManager.getSourceClusterId(mirrorName);
    }

    @Override
    public String getSourceBootstrap(String mirrorName) {
        return metadataManager.getSourceBootstrap(mirrorName);
    }

    @Override
    public Set<String> getConfiguredMirrors() {
        return metadataManager.getConfiguredMirrors();
    }

    @Override
    public Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        return metadataManager.getMirrorStates(mirrorName);
    }

    @Override
    public Set<String> getConfiguredTopics(String mirrorName, boolean includePaused, boolean includeStopped) {
        return metadataManager.getConfiguredTopics(mirrorName, includePaused, includeStopped);
    }

    @Override
    public int getActiveTopicCount(String mirrorName) {
        return metadataManager.getActiveTopicCount(mirrorName);
    }

    // Cache cleanup

    @Override
    public void removeCachedMirror(String mirrorName) {
        metadataManager.removeCachedMirror(mirrorName);
    }

    @Override
    public void removeStateForPartitions(Set<TopicPartition> partitions) {
        metadataManager.removeStateForPartitions(partitions);
    }

    // Validation

    @Override
    public void validateStartMirrorStates(StartMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateStartMirrorStates(data, callback);
    }

    @Override
    public void validateStopMirrorStates(StopMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateStopMirrorStates(data, callback);
    }

    @Override
    public void validatePauseMirrorStates(PauseMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validatePauseMirrorStates(data, callback);
    }

    @Override
    public void validateResumeMirrorStates(ResumeMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateResumeMirrorStates(data, callback);
    }

    @Override
    public void validateDeleteMirrorStates(DeleteClusterMirrorRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateDeleteMirrorStates(data, callback);
    }

    // LME

    @Override
    public Map<String, Map<TopicPartition, Integer>> processLastMirrorEpochLookup(
        Map<String, Map<String, Set<Integer>>> mirrorPartitions
    ) {
        return metadataManager.processLastMirrorEpochLookup(mirrorPartitions);
    }

    // Metadata scheduling

    @Override
    public void scheduleMetadataRefresh(long intervalMs) {
        metadataManager.scheduleMetadataRefresh(intervalMs);
    }

    // Topic resolution

    @Override
    public Uuid getTopicId(String topicName) {
        return metadataCache.getTopicId(topicName);
    }

    @Override
    public Optional<String> getTopicName(Uuid topicId) {
        return metadataCache.getTopicName(topicId);
    }

    // Leader check

    @Override
    public int getLeaderForPartition(String topic, int partition) {
        return metadataCache.getLeaderAndIsr(topic, partition)
            .map(lai -> lai.leader())
            .orElse(-1);
    }
}
