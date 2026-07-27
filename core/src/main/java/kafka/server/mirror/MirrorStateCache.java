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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.mirror.MirrorPartition;
import org.apache.kafka.coordinator.mirror.MirrorPartitionKey;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Thread-safe cache for mirror partition state, source leaders,
 * pending topic creations, and pending leader epoch bumps.
 */
public class MirrorStateCache {

    private final Map<MirrorPartitionKey, MirrorPartition> partitions = new ConcurrentHashMap<>();
    private final Map<String, Map<TopicPartition, SourceLeader>> sourceLeaders = new ConcurrentHashMap<>();
    private final Set<String> pendingTopicCreations = ConcurrentHashMap.newKeySet();
    private final Set<PendingLeaderEpochBump> pendingEpochBumps = ConcurrentHashMap.newKeySet();

    private MirrorStateCache() {
    }

    public static MirrorStateCache empty() {
        return new MirrorStateCache();
    }

    // -- Partition cache operations --

    public MirrorPartition get(MirrorPartitionKey key) {
        return partitions.get(key);
    }

    /** Merges a coordinator response into the local cache entry for the given key. */
    public void merge(MirrorPartitionKey key, byte state, int lastMirrorEpoch,
                      String errorMessage, int retryAttempt, byte previousState) {
        partitions.compute(key, (k, existing) -> {
            MirrorPartition result = MirrorPartition.orEmpty(existing);
            if (state != -1) result = result.withState(MirrorPartitionState.fromValue(state));
            if (lastMirrorEpoch != -1) result = result.withLastMirrorEpoch(lastMirrorEpoch);
            if (state == MirrorPartitionState.FAILED.value()) {
                result = result.withError(errorMessage, retryAttempt, MirrorPartitionState.fromValue(previousState));
            }
            return result;
        });
    }

    public void setPartitionState(MirrorPartitionKey key, MirrorPartitionState newState) {
        partitions.compute(key, (k, existing) -> MirrorPartition.orEmpty(existing).withState(newState));
    }

    public void setLastMirrorEpoch(MirrorPartitionKey key, int epoch) {
        partitions.compute(key, (k, existing) -> MirrorPartition.orEmpty(existing).withLastMirrorEpoch(epoch));
    }

    public void remove(MirrorPartitionKey key) {
        partitions.remove(key);
    }

    public void removeMirror(String mirrorName) {
        partitions.keySet().removeIf(key -> key.mirrorName().equals(mirrorName));
    }

    /** Clears all caches. */
    public void clear() {
        partitions.clear();
        sourceLeaders.clear();
        pendingTopicCreations.clear();
        pendingEpochBumps.clear();
    }

    public void clearPartition(int partitionIndex, int numPartitions) {
        partitions.keySet().removeIf(key ->
            Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions == partitionIndex);
    }

    public long partitionStateCount(MirrorPartitionState state) {
        return partitions.values().stream()
                .filter(entry -> entry.state() == state)
                .count();
    }

    public void forEach(BiConsumer<MirrorPartitionKey, MirrorPartition> action) {
        partitions.forEach(action);
    }

    public Set<MirrorPartitionKey> keySet() {
        return partitions.keySet();
    }

    // -- Failed info operations --

    public void setFailedInfo(MirrorPartitionKey key, String errorMessage, int retryAttempt, MirrorPartitionState prevState) {
        partitions.compute(key, (k, existing) -> MirrorPartition.orEmpty(existing).withError(errorMessage, retryAttempt, prevState));
    }

    public void clearFailedInfo(MirrorPartitionKey key) {
        partitions.computeIfPresent(key, (k, existing) -> existing.withError(null, 0, null));
    }

    /**
     * Updates failed partition info for a state transition. Sets retry attempt and previous
     * state on FAILED transitions, clears on LOG_TRUNCATION/STOPPED/PAUSED. Preserves the
     * non-retryable marker when a FAILED partition is re-transitioned with nonRetryable=false.
     */
    public void updateFailedInfo(MirrorPartitionKey key, MirrorPartitionState state,
                                 MirrorPartitionState newState, String errorMessage, boolean nonRetryable) {
        if (newState == MirrorPartitionState.FAILED) {
            MirrorPartition existing = MirrorPartition.orEmpty(get(key));
            int attempt = existing.nextAttempt(nonRetryable);
            MirrorPartitionState previousState = existing.resolvePreviousState(state);
            setFailedInfo(key, errorMessage, attempt, previousState);
        } else if (newState == MirrorPartitionState.LOG_TRUNCATION
                || newState == MirrorPartitionState.STOPPED
                || newState == MirrorPartitionState.PAUSED) {
            clearFailedInfo(key);
        }
    }

    // -- Source leader operations --

    public void updateSourceLeader(String mirrorName, TopicPartition tp, SourceLeader leader) {
        sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>()).put(tp, leader);
    }

    /** Resolves the cached source leader for a partition, throwing if no metadata is available. */
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

    public void removeSourceLeaders(String mirrorName) {
        sourceLeaders.remove(mirrorName);
    }

    public Map<TopicPartition, SourceLeader> getOrCreateSourceLeaders(String mirrorName) {
        return sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>());
    }

    public Map<TopicPartition, SourceLeader> getSourceLeaders(String mirrorName) {
        return sourceLeaders.get(mirrorName);
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
        pendingEpochBumps.add(bump);
    }

    public Set<PendingLeaderEpochBump> getPendingEpochBumps() {
        return pendingEpochBumps;
    }

    public void removePendingEpochBumps(Set<TopicPartition> partitions) {
        pendingEpochBumps.removeIf(bump -> {
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
