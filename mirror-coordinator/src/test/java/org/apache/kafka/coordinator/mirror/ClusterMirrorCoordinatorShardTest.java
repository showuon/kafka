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
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterMirrorCoordinatorShardTest {
    private static final String MIRROR_NAME = "my-mirror";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final String TOPIC_NAME = "my-topic";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC_NAME, 0);
    private static final TopicPartition TP1 = new TopicPartition(TOPIC_NAME, 1);

    private SnapshotRegistry snapshotRegistry;
    private TestCoreBridge coreBridge;
    private ClusterMirrorCoordinatorShard shard;

    @BeforeEach
    void setUp() {
        LogContext logContext = new LogContext();
        snapshotRegistry = new SnapshotRegistry(logContext);
        coreBridge = new TestCoreBridge();
        shard = new ClusterMirrorCoordinatorShard.Builder(coreBridge, 1)
                .withLogContext(logContext)
                .withTopicPartition(new TopicPartition(MIRROR_STATE_TOPIC_NAME, 0))
                .withSnapshotRegistry(snapshotRegistry)
                .build();
    }

    @Test
    void testWritePartitionStateIncrementsEpoch() {
        CoordinatorResult<Void, CoordinatorRecord> result =
                shard.writePartitionState(MIRROR_NAME, TP0, MirrorPartitionState.LOG_TRUNCATION, -1, null, false);

        assertEquals(1, result.records().size());
        MirrorPartitionStateValue val = (MirrorPartitionStateValue) result.records().get(0).value().message();
        assertEquals(1, val.stateEpoch());
    }

    @Test
    void testWritePartitionStateIncrementsEpochSequentially() {
        CoordinatorResult<Void, CoordinatorRecord> first =
                shard.writePartitionState(MIRROR_NAME, TP0, MirrorPartitionState.LOG_TRUNCATION, -1, null, false);
        replayRecords(first);

        CoordinatorResult<Void, CoordinatorRecord> second =
                shard.writePartitionState(MIRROR_NAME, TP0, MirrorPartitionState.MIRRORING, -1, null, false);

        assertEquals(1, second.records().size());
        MirrorPartitionStateValue val = (MirrorPartitionStateValue) second.records().get(0).value().message();
        assertEquals(2, val.stateEpoch());
    }

    @Test
    void testWritePartitionStateFencedEpoch() {
        replayState(TP0, MirrorPartitionState.LOG_TRUNCATION, 5);

        assertThrows(FencedStateEpochException.class, () ->
                shard.writePartitionState(MIRROR_NAME, TP0, MirrorPartitionState.MIRRORING, 3, null, false));
    }

    @Test
    void testWritePartitionStateFencedProducesNoRecords() {
        replayState(TP0, MirrorPartitionState.LOG_TRUNCATION, 5);

        try {
            shard.writePartitionState(MIRROR_NAME, TP0, MirrorPartitionState.MIRRORING, 3, null, false);
        } catch (FencedStateEpochException ignored) {
        }

        MirrorPartition mp = coreBridge.getPartition(
                MirrorPartitionKey.of(MIRROR_NAME, TOPIC_ID, 0));
        assertEquals(MirrorPartitionState.LOG_TRUNCATION, mp.state());
        assertEquals(5, mp.stateEpoch());
    }

    @Test
    void testWritePartitionStateSkipsCheckWhenMinusOne() {
        replayState(TP0, MirrorPartitionState.LOG_TRUNCATION, 10);

        CoordinatorResult<Void, CoordinatorRecord> result =
                shard.writePartitionState(MIRROR_NAME, TP0, MirrorPartitionState.MIRRORING, -1, null, false);

        assertEquals(1, result.records().size());
        MirrorPartitionStateValue val = (MirrorPartitionStateValue) result.records().get(0).value().message();
        assertEquals(11, val.stateEpoch());
    }

    @Test
    void testWriteStateBatchedFencing() {
        replayState(TP0, MirrorPartitionState.LOG_TRUNCATION, 5);
        replayState(TP1, MirrorPartitionState.LOG_TRUNCATION, 3);

        Map<String, Set<ClusterMirrorCoordinatorService.MirrorStateWrite>> mirrorStates = Map.of(
                TOPIC_NAME, Set.of(
                        new ClusterMirrorCoordinatorService.MirrorStateWrite(0, MirrorPartitionState.MIRRORING, 2, -1),
                        new ClusterMirrorCoordinatorService.MirrorStateWrite(1, MirrorPartitionState.MIRRORING, 3, -1)));

        CoordinatorResult<Map<TopicPartition, ClusterMirrorCoordinatorShard.PartitionWriteResult>, CoordinatorRecord> result =
                shard.writeState(MIRROR_NAME, mirrorStates);

        Map<TopicPartition, ClusterMirrorCoordinatorShard.PartitionWriteResult> results = result.response();

        assertEquals(Errors.FENCED_STATE_EPOCH, results.get(TP0).error());
        assertEquals(-1, results.get(TP0).stateEpoch());

        assertEquals(Errors.NONE, results.get(TP1).error());
        assertEquals(4, results.get(TP1).stateEpoch());

        assertFalse(result.records().isEmpty());
    }

    @Test
    void testReplayPopulatesStateEpoch() {
        replayState(TP0, MirrorPartitionState.LOG_TRUNCATION, 7);

        MirrorPartition mp = coreBridge.getPartition(
                MirrorPartitionKey.of(MIRROR_NAME, TOPIC_ID, 0));
        assertEquals(7, mp.stateEpoch());
        assertEquals(MirrorPartitionState.LOG_TRUNCATION, mp.state());
    }

    @Test
    void testReadStateIncludesEpoch() {
        replayState(TP0, MirrorPartitionState.LOG_TRUNCATION, 4);

        ReadMirrorStatesResponseData data = shard.readState(MIRROR_NAME,
                Map.of(TOPIC_NAME, Set.of(0)));

        ReadMirrorStatesResponseData.PartitionResult pr =
                data.topics().get(0).partitions().get(0);
        assertEquals(4, pr.stateEpoch());
        assertEquals(MirrorPartitionState.LOG_TRUNCATION.value(), pr.state());
    }

    @Test
    void testTombstoneRemovesEpoch() {
        replayState(TP0, MirrorPartitionState.LOG_TRUNCATION, 5);

        MirrorPartitionStateKey key = new MirrorPartitionStateKey()
                .setMirrorName(MIRROR_NAME)
                .setTopicId(TOPIC_ID)
                .setPartition(0);
        shard.replay(0, -1, (short) -1,
                CoordinatorRecord.tombstone(key));

        MirrorPartition mp = coreBridge.getPartition(
                MirrorPartitionKey.of(MIRROR_NAME, TOPIC_ID, 0));
        assertTrue(mp == null || mp.stateEpoch() == 0);
    }

    @Test
    void testBrokersRaceSimulation() {
        replayState(TP0, MirrorPartitionState.LOG_TRUNCATION, 5);

        CoordinatorResult<Void, CoordinatorRecord> firstWrite =
                shard.writePartitionState(MIRROR_NAME, TP0, MirrorPartitionState.FAILED, 5, "error", false);
        assertEquals(1, firstWrite.records().size());

        assertThrows(FencedStateEpochException.class, () ->
                shard.writePartitionState(MIRROR_NAME, TP0, MirrorPartitionState.MIRRORING, 5, null, false));
    }

    private void replayRecords(CoordinatorResult<?, CoordinatorRecord> result) {
        for (CoordinatorRecord record : result.records()) {
            shard.replay(0, -1, (short) -1, record);
        }
    }

    private void replayState(TopicPartition tp, MirrorPartitionState state, int stateEpoch) {
        MirrorPartitionStateKey key = new MirrorPartitionStateKey()
                .setMirrorName(MIRROR_NAME)
                .setTopicId(TOPIC_ID)
                .setPartition(tp.partition());
        MirrorPartitionStateValue val = new MirrorPartitionStateValue()
                .setState(state.value())
                .setStateEpoch(stateEpoch)
                .setPreviousState(MirrorPartitionState.UNKNOWN.value());
        shard.replay(0, -1, (short) -1,
                CoordinatorRecord.record(key, new ApiMessageAndVersion(val, (short) 0)));
    }

    private static class TestCoreBridge implements CoreBridge {
        private final Map<MirrorPartitionKey, MirrorPartition> partitions = new ConcurrentHashMap<>();

        @Override
        public void initialize(CoordinatorWriter coordinatorWriter,
                               Function<MirrorPartitionKey, Integer> coordPartFinder) {
        }

        @Override
        public void closeSourceAdmins() {
        }

        @Override
        public void onShardLoaded() {
        }

        @Override
        public void onShardUnloaded(int coordPartition, int coordPartitionCount) {
        }

        @Override
        public MirrorPartition getPartition(MirrorPartitionKey key) {
            return partitions.get(key);
        }

        @Override
        public void setPartition(MirrorPartitionKey key, MirrorPartition partition) {
            partitions.put(key, partition);
        }

        @Override
        public void removePartition(MirrorPartitionKey key) {
            partitions.remove(key);
        }

        @Override
        public void updateFailedInfo(MirrorPartitionKey key, MirrorPartitionState curState,
                                     MirrorPartitionState newState, String errorMessage, boolean nonRetryable) {
            if (newState == MirrorPartitionState.FAILED) {
                MirrorPartition existing = MirrorPartition.orEmpty(getPartition(key));
                int attempt = existing.nextAttempt(nonRetryable);
                MirrorPartitionState previousState = existing.resolvePrevState(curState);
                partitions.compute(key, (k, e) ->
                        MirrorPartition.orEmpty(e).withError(errorMessage, attempt, previousState));
            } else if (newState == MirrorPartitionState.LOG_TRUNCATION
                    || newState == MirrorPartitionState.STOPPED
                    || newState == MirrorPartitionState.PAUSED) {
                partitions.computeIfPresent(key, (k, existing) -> existing.clearError());
            }
        }

        @Override
        public void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch) {
        }

        @Override
        public Uuid getTopicId(String topicName) {
            return TOPIC_ID;
        }

        @Override
        public Optional<String> getTopicName(Uuid topicId) {
            return Optional.of(TOPIC_NAME);
        }
    }
}
