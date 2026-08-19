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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.coordinator.mirror.MirrorPartition;
import org.apache.kafka.coordinator.mirror.MirrorPartitionKey;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe cache for mirror partition state, source leaders,
 * pending topic creations, and pending leader epoch bumps.
 */
public class MirrorStateCache {
    private final Map<MirrorPartitionKey, MirrorPartition> partitions = new ConcurrentHashMap<>();
    private final Map<String, Map<TopicPartition, SourceLeader>> sourceLeaders = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> sourceDeletions = new ConcurrentHashMap<>();
    private final Set<Integer> loadedCoordPartitions = ConcurrentHashMap.newKeySet();
    private final Set<String> pendingTopicCreations = ConcurrentHashMap.newKeySet();
    private final Set<PendingLeaderEpochBump> pendingLederEpochBumps = ConcurrentHashMap.newKeySet();

    public static MirrorStateCache empty() {
        return new MirrorStateCache();
    }

    private MirrorStateCache() {
    }

    public void clear() {
        partitions.clear();
        sourceLeaders.clear();
        sourceDeletions.clear();
        loadedCoordPartitions.clear();
        pendingTopicCreations.clear();
        pendingLederEpochBumps.clear();
    }

    // -- Partition cache operations --

    public MirrorPartition getPartition(MirrorPartitionKey key) {
        return partitions.get(key);
    }

    public void setPartition(MirrorPartitionKey key, MirrorPartition partition) {
        partitions.put(key, partition);
    }

    public void mergePartition(MirrorPartitionKey key, byte state, int stateEpoch, int lastMirrorEpoch,
                               String errorMessage, int retryAttempt, byte previousState) {
        partitions.compute(key, (k, existing) -> {
            MirrorPartition result = MirrorPartition.orEmpty(existing);
            if (state != -1) result = result.withState(MirrorPartitionState.fromValue(state));
            if (stateEpoch >= 0) result = result.withStateEpoch(stateEpoch);
            if (lastMirrorEpoch != -1) result = result.withLastMirrorEpoch(lastMirrorEpoch);
            if (state == MirrorPartitionState.FAILED.value()) {
                result = result.withError(errorMessage, retryAttempt, MirrorPartitionState.fromValue(previousState));
            }
            return result;
        });
    }

    public void removePartition(MirrorPartitionKey key) {
        partitions.remove(key);
    }

    public void clearPartition(int coordPartition, int coordPartitionCount) {
        partitions.keySet().removeIf(key ->
            key.coordinatorPartition(coordPartitionCount) == coordPartition);
    }

    public long partitionStateCount(MirrorPartitionState state) {
        return partitions.values().stream()
                .filter(entry -> entry.state() == state)
                .count();
    }

    public Set<MirrorPartitionKey> partitionKeys() {
        return partitions.keySet();
    }

    public void setLastMirrorEpoch(MirrorPartitionKey key, int epoch) {
        partitions.compute(key, (k, existing) -> MirrorPartition.orEmpty(existing).withLastMirrorEpoch(epoch));
    }

    public void removeMirror(String mirrorName) {
        partitions.keySet().removeIf(key -> key.mirrorName().equals(mirrorName));
        sourceDeletions.remove(mirrorName);
    }

    public void updateFailedInfo(MirrorPartitionKey key, MirrorPartitionState currentState,
                                 MirrorPartitionState newState, String errorMessage, boolean isPermFailure) {
        if (newState == MirrorPartitionState.FAILED) {
            MirrorPartition existing = MirrorPartition.orEmpty(getPartition(key));
            int attempt = existing.nextAttempt(isPermFailure);
            MirrorPartitionState previousState = existing.resolvePrevState(currentState);
            partitions.compute(key, (k, e) -> MirrorPartition.orEmpty(e).withError(errorMessage, attempt, previousState));
        } else if (newState == MirrorPartitionState.LOG_ALIGNMENT
                || newState == MirrorPartitionState.STOPPED
                || newState == MirrorPartitionState.PAUSED) {
            clearFailedInfo(key);
        }
    }

    public void clearFailedInfo(MirrorPartitionKey key) {
        partitions.computeIfPresent(key, (k, existing) -> existing.clearError());
    }

    // -- Source leader operations --

    public Map<TopicPartition, SourceLeader> getSourceLeaders(String mirrorName) {
        return sourceLeaders.get(mirrorName);
    }

    public SourceLeader resolveSourceLeader(String mirrorName, TopicPartition tp) {
        var partitionLeaders = sourceLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            SourceLeader leader = partitionLeaders.get(tp);
            if (leader != null) {
                return leader;
            }
        }
        throw new IllegalStateException("No source cluster metadata available " +
                "for mirror " + mirrorName + " partition:" + tp);
    }

    public void updateSourceLeader(String mirrorName, TopicPartition tp, SourceLeader leader) {
        sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>()).put(tp, leader);
    }

    public void removeSourceLeaders(String mirrorName) {
        sourceLeaders.remove(mirrorName);
    }

    // -- Source topic deletion operations --

    public boolean addSourceDeletion(String mirrorName, String topic) {
        return sourceDeletions.computeIfAbsent(mirrorName, k -> ConcurrentHashMap.newKeySet()).add(topic);
    }

    public boolean isSourceDeletion(String mirrorName, String topic) {
        Set<String> topics = sourceDeletions.get(mirrorName);
        return topics != null && topics.contains(topic);
    }

    public void removeSourceDeletion(String mirrorName, String topic) {
        Set<String> topics = sourceDeletions.get(mirrorName);
        if (topics != null) {
            topics.remove(topic);
        }
    }

    // -- Loaded coordinator shard operations --

    public boolean isShardLoaded(int coordPartition) {
        return loadedCoordPartitions.contains(coordPartition);
    }

    public void addLoadedShard(int coordPartition) {
        loadedCoordPartitions.add(coordPartition);
    }

    public void removeLoadedShard(int coordPartition) {
        loadedCoordPartitions.remove(coordPartition);
    }

    // -- Pending topic creation operations --

    public boolean addPendingTopicCreation(String topic) {
        return pendingTopicCreations.add(topic);
    }

    public void removePendingTopicCreation(String topic) {
        pendingTopicCreations.remove(topic);
    }

    // -- Pending leader epoch bump operations --

    public void addPendingEpochBump(PendingLeaderEpochBump bump) {
        pendingLederEpochBumps.add(bump);
    }

    public Set<PendingLeaderEpochBump> getPendingLederEpochBumps() {
        return pendingLederEpochBumps;
    }

    public void clearPendingLeaderEpochBumps(Set<TopicPartition> partitions) {
        pendingLederEpochBumps.removeIf(bump -> {
            bump.partitionToEpoch().keySet().removeAll(partitions);
            if (bump.partitionToEpoch().isEmpty()) {
                bump.future().cancel(false);
                return true;
            }
            return false;
        });
    }

    /** Cached source cluster leader node and epoch for a mirror partition. */
    public record SourceLeader(Node node, int leaderEpoch) { }

    /** Pending leader epoch bump request with the future that completes when the bump is observed in metadata. */
    public record PendingLeaderEpochBump(CompletableFuture<Void> future, Map<TopicPartition, Integer> partitionToEpoch) { }
}
