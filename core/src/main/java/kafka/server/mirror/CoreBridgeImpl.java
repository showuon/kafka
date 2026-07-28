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
package kafka.server.mirror;

import kafka.server.ReplicaManager;

import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorService.MirrorStateWrite;
import org.apache.kafka.coordinator.mirror.CoreBridge;
import org.apache.kafka.coordinator.mirror.MirrorPartition;
import org.apache.kafka.coordinator.mirror.MirrorPartitionKey;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.MetadataCache;
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

import scala.jdk.javaapi.CollectionConverters;

public class CoreBridgeImpl implements CoreBridge {
    private final MirrorMetadataManager metadataManager;
    private final MetadataCache metadataCache;
    private final ReplicaManager replicaManager;

    public CoreBridgeImpl(MirrorMetadataManager metadataManager, MetadataCache metadataCache,
                          ReplicaManager replicaManager) {
        this.metadataManager = metadataManager;
        this.metadataCache = metadataCache;
        this.replicaManager = replicaManager;
    }

    @Override
    public void initialize(
        StateTransitioner stateTransitioner,
        Consumer<String> tombstoneWriter,
        Function<MirrorPartitionKey, Integer> coordPartitionByKeyFinder,
        Function<String, Integer> coordPartitionByNameFinder
    ) {
        metadataManager.initialize(
            stateTransitioner::transitionTo,
            tombstoneWriter,
            coordPartitionByKeyFinder,
            coordPartitionByNameFinder);
    }

    @Override
    public void closeSourceAdmins() {
        metadataManager.closeSourceAdmins();
    }

    @Override
    public void onShardLoaded() {
        metadataManager.processAllStateTransitions();
    }

    @Override
    public void onShardUnloaded(int coordPartition, int coordPartitionCount) {
        metadataManager.clearPartition(coordPartition, coordPartitionCount);
    }

    @Override
    public MirrorPartition getPartition(MirrorPartitionKey key) {
        return metadataManager.getPartition(key);
    }

    @Override
    public void setPartition(MirrorPartitionKey key, MirrorPartition partition) {
        metadataManager.setPartition(key, partition);
    }

    @Override
    public void removePartition(MirrorPartitionKey key) {
        metadataManager.removePartition(key);
    }

    @Override
    public void updateFailedInfo(
        MirrorPartitionKey key,
        MirrorPartitionState curState,
        MirrorPartitionState newState,
        String errorMessage,
        boolean nonRetryable
    ) {
        metadataManager.updateFailedInfo(key, curState, newState, errorMessage, nonRetryable);
    }

    @Override
    public void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch) {
        metadataManager.setLastMirrorEpoch(mirrorName, topic, partition, epoch);
    }

    @Override
    public void writeStatesToRemoteCoordinator(
        String mirrorName,
        Map<String, Set<MirrorStateWrite>> topicMetadata,
        Set<String> stoppedTopics,
        Consumer<WriteMirrorStatesResponse> callback
    ) {
        metadataManager.writeStatesToRemoteCoordinator(mirrorName, topicMetadata, stoppedTopics, callback);
    }

    @Override
    public CompletionStage<Map<TopicPartition, Integer>> sendLastMirrorEpochLookup(
        String mirrorName,
        TopicPartition tp,
        Collection<ClusterMirrorListing> sourceMirrors
    ) {
        return metadataManager.sendLastMirrorEpochLookup(mirrorName, tp, sourceMirrors);
    }

    @Override
    public CompletableFuture<Void> bumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs) {
        return metadataManager.bumpLeaderEpochs(partitionMinEpochs);
    }

    @Override
    public CompletableFuture<Void> scheduleBumpLeaderEpoch(String mirrorName, TopicPartition tp) {
        return metadataManager.scheduleBumpLeaderEpoch(mirrorName, tp);
    }

    @Override
    public Collection<ClusterMirrorListing> listSourceClusterMirrors(String mirrorName) {
        return metadataManager.listSourceClusterMirrors(mirrorName);
    }

    @Override
    public boolean hasMirrorLoop(String mirrorName, TopicPartition tp, Collection<ClusterMirrorListing> sourceMirrors) {
        return metadataManager.hasMirrorLoop(mirrorName, tp, sourceMirrors);
    }

    @Override
    public String getSourceClusterId(String mirrorName) {
        return metadataManager.getSourceClusterId(mirrorName);
    }

    @Override
    public Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        return metadataManager.getMirrorStates(mirrorName);
    }

    @Override
    public void removeMirror(String mirrorName) {
        metadataManager.removeMirror(mirrorName);
    }

    @Override
    public void removePendingEpochBumps(Set<TopicPartition> partitions) {
        metadataManager.removePendingEpochBumps(partitions);
    }

    @Override
    public Uuid getTopicId(String topicName) {
        return metadataCache.getTopicId(topicName);
    }

    @Override
    public Optional<String> getTopicName(Uuid topicId) {
        return metadataCache.getTopicName(topicId);
    }

    @Override
    public int getLeaderForPartition(String topic, int partition) {
        return metadataCache.getLeaderAndIsr(topic, partition)
                .map(LeaderAndIsr::leader)
                .orElse(-1);
    }

    @Override
    public void maybeCreateMirrorFetchers(String mirrorName, Set<TopicPartition> partitions) {
        replicaManager.maybeCreateMirrorFetchers(mirrorName, partitions);
    }

    @Override
    public void removeFetcherForPartitions(Set<TopicPartition> partitions) {
        replicaManager.mirrorFetcherManager().removeFetcherForPartitions(
            CollectionConverters.asScala(partitions));
    }

    @Override
    public OptionalInt getLatestEpoch(TopicPartition tp) {
        var logOpt = replicaManager.getPartitionOrException(tp).log();
        if (logOpt.isDefined()) {
            return OptionalInt.of(logOpt.get().latestEpoch().orElse(-1));
        }
        return OptionalInt.empty();
    }

    @Override
    public Map<TopicPartition, Integer> getLatestLocalEpoch(TopicPartition tp) {
        int epoch = replicaManager.logManager().getLog(tp, false).get().latestEpoch().orElse(-1);
        return Map.of(tp, epoch);
    }

    @Override
    public void maybeTruncateForLeaderEpoch(Map<TopicPartition, Integer> epochs, Consumer<TopicPartition> callback) {
        replicaManager.maybeTruncateForLeaderEpoch(epochs, callback);
    }

    @Override
    public CompletableFuture<Void> abortOngoingTransactions(TopicPartition tp) {
        return metadataManager.abortOngoingTransactions(tp);
    }

    @Override
    public CompletableFuture<Void> appendPidResetBarrier(TopicPartition tp, String sourceClusterId, long timestampMs) {
        return metadataManager.appendPidResetBarrier(tp, sourceClusterId, timestampMs);
    }
}
