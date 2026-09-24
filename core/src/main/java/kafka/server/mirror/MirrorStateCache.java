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

import org.apache.kafka.common.EpochOffset;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.mirror.MirrorPartitionKey;
import org.apache.kafka.server.mirror.MirrorPartitionMetadata;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe cache for mirroring metadata.
 */
public class MirrorStateCache {
    private final Map<MirrorPartitionKey, MirrorPartitionMetadata> partMetadata = new ConcurrentHashMap<>();
    private final Map<String, Map<TopicPartition, SourceLeader>> sourceLeaders = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> sourceDeletions = new ConcurrentHashMap<>();
    private final Map<TopicPartition, MirrorPartitionState> pendingStateTransitions = new ConcurrentHashMap<>();
    private final Set<String> pendingTopicCreations = ConcurrentHashMap.newKeySet();
    private final Set<PendingLeaderEpochBump> pendingLeaderEpochBumps = ConcurrentHashMap.newKeySet();

    public static MirrorStateCache empty() {
        return new MirrorStateCache();
    }

    private MirrorStateCache() {
    }

    public void clear() {
        partMetadata.clear();
        sourceLeaders.clear();
        sourceDeletions.clear();
        pendingStateTransitions.clear();
        pendingTopicCreations.clear();
        pendingLeaderEpochBumps.clear();
    }

    public MirrorPartitionMetadata getPartitionMetadata(MirrorPartitionKey key) {
        return partMetadata.get(key);
    }

    public void setPartitionMetadata(MirrorPartitionKey key, MirrorPartitionMetadata meta) {
        partMetadata.put(key, meta);
    }

    public void mergePartitionMetadata(MirrorPartitionKey key, byte state, int stateEpoch, EpochOffset lastMirrorPosition,
                                       String errorMessage, int retryAttempt, byte previousState) {
        partMetadata.compute(key, (k, existing) -> {
            MirrorPartitionMetadata result = MirrorPartitionMetadata.orEmpty(existing);
            if (state != -1) result = result.withState(MirrorPartitionState.fromValue(state));
            if (stateEpoch >= 0) result = result.withStateEpoch(stateEpoch);
            if (lastMirrorPosition.epoch() != -1) result = result.withLastMirrorEpoch(lastMirrorPosition.epoch());
            if (lastMirrorPosition.offset() != -1) result = result.withLastMirrorOffset(lastMirrorPosition.offset());
            if (state == MirrorPartitionState.FAILED.value()) {
                result = result.withError(errorMessage, retryAttempt, MirrorPartitionState.fromValue(previousState));
            }
            return result;
        });
    }

    public void removePartitionMetadata(MirrorPartitionKey key) {
        partMetadata.remove(key);
    }

    public void clearPartitionMetadata(int coordPartition, int numPartitions) {
        partMetadata.keySet().removeIf(key ->
            key.coordinatorPartition(numPartitions) == coordPartition);
    }

    public long getPartitionStateCount(MirrorPartitionState state) {
        return partMetadata.values().stream()
                .filter(entry -> entry.state() == state)
                .count();
    }

    public Set<MirrorPartitionKey> getPartitionKeys() {
        return partMetadata.keySet();
    }

    public void setLastMirrorPosition(MirrorPartitionKey key, EpochOffset lastMirrorPosition) {
        partMetadata.compute(key, (k, existing) -> MirrorPartitionMetadata.orEmpty(existing).withLastMirrorPosition(lastMirrorPosition));
    }

    public void removeMirror(String mirrorName) {
        partMetadata.keySet().removeIf(key -> key.mirrorName().equals(mirrorName));
        sourceDeletions.remove(mirrorName);
    }

    public void updateFailureDetails(MirrorPartitionKey key, MirrorPartitionState curState,
                                     MirrorPartitionState newState, String errorMessage,
                                     boolean nonRetryable, int maxAttempts) {
        MirrorPartitionMetadata existing = MirrorPartitionMetadata.orEmpty(getPartitionMetadata(key));
        if (newState == MirrorPartitionState.FAILED) {
            int attempt = existing.nextAttempt(nonRetryable, maxAttempts);
            MirrorPartitionState previousState = existing.resolvePrevState(curState);
            partMetadata.compute(key, (k, e) -> MirrorPartitionMetadata.orEmpty(e).withError(errorMessage, attempt, previousState));
        } else if ((curState != MirrorPartitionState.FAILED && curState != newState)
                || newState == MirrorPartitionState.STOPPED
                || newState == MirrorPartitionState.PAUSED) {
            // Clean up the state when:
            // 1. new state is STOPPED or PAUSED state
            // 2. there is state change, but not change from/to FAILED
            // we already filter out the newState == FAILED case above, so skip the check
            // 3. For MIRRORING, it'll clean up after the first successful fetch response in MirrorFetcherThread.
            clearFailureDetails(key);
        } else {
            // Update the error message to make sure it is up-to-date
            partMetadata.compute(key, (k, e) ->
                    existing.withError(errorMessage, existing.retryAttempt(), existing.prevState()));
        }
    }

    public void clearFailureDetails(MirrorPartitionKey key) {
        partMetadata.computeIfPresent(key, (k, existing) -> existing.clearError());
    }

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

    public MirrorPartitionState pendingStateTransition(TopicPartition tp) {
        return pendingStateTransitions.get(tp);
    }

    public void addPendingStateTransition(TopicPartition tp, MirrorPartitionState state) {
        pendingStateTransitions.put(tp, state);
    }

    public void removePendingStateTransition(TopicPartition tp) {
        pendingStateTransitions.remove(tp);
    }

    public boolean addPendingTopicCreation(String topic) {
        return pendingTopicCreations.add(topic);
    }

    public void removePendingTopicCreation(String topic) {
        pendingTopicCreations.remove(topic);
    }

    public void addPendingEpochBump(PendingLeaderEpochBump bump) {
        pendingLeaderEpochBumps.add(bump);
    }

    public Set<PendingLeaderEpochBump> getPendingLeaderEpochBumps() {
        return pendingLeaderEpochBumps;
    }

    public void clearPendingLeaderEpochBumps(Set<TopicPartition> partitions) {
        pendingLeaderEpochBumps.removeIf(bump -> {
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
