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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.mirror.bridge.MirrorMetadataManagerShardBridge;
import org.apache.kafka.coordinator.mirror.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsKey;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsValue;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The shard (state machine) for the cluster mirror coordinator.
 * One instance per __mirror_state partition, managed by the CoordinatorRuntime.
 */
public class ClusterMirrorCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    private final Logger log;
    private final MirrorMetadataManagerShardBridge metadataManagerBridge;
    private final TopicPartition topicPartition;
    private final int numPartitions;

    public static class Builder implements CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> {
        private final MirrorMetadataManagerShardBridge metadataManagerBridge;
        private final int numPartitions;
        private LogContext logContext;
        private TopicPartition topicPartition;

        public Builder(MirrorMetadataManagerShardBridge metadataManagerBridge, int numPartitions) {
            this.metadataManagerBridge = metadataManagerBridge;
            this.numPartitions = numPartitions;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withTime(Time time) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withExecutor(CoordinatorExecutor<CoordinatorRecord> executor) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            return this;
        }

        @Override
        public ClusterMirrorCoordinatorShard build() {
            if (logContext == null) throw new IllegalArgumentException("LogContext must not be null.");
            if (topicPartition == null) throw new IllegalArgumentException("TopicPartition must not be null.");
            return new ClusterMirrorCoordinatorShard(logContext, metadataManagerBridge, topicPartition, numPartitions);
        }
    }

    private ClusterMirrorCoordinatorShard(
        LogContext logContext,
        MirrorMetadataManagerShardBridge metadataManagerBridge,
        TopicPartition topicPartition,
        int numPartitions
    ) {
        this.log = logContext.logger(ClusterMirrorCoordinatorShard.class);
        this.metadataManagerBridge = metadataManagerBridge;
        this.topicPartition = topicPartition;
        this.numPartitions = numPartitions;
    }

    // ---------------------------------------------------------------
    // Lifecycle hooks (called by the runtime)
    // ---------------------------------------------------------------

    @Override
    public void replay(long offset, long producerId, short producerEpoch, CoordinatorRecord record) {
        ApiMessage key = record.key();
        ApiMessageAndVersion value = record.value();

        try {
            switch (CoordinatorRecordType.fromId(key.apiKey())) {
                case MIRROR_PARTITION_STATE:
                    replayPartitionState((MirrorPartitionStateKey) key, value);
                    break;
                case LAST_MIRROR_EPOCHS:
                    replayLastMirrorEpochs((LastMirrorEpochsKey) key, value);
                    break;
                default:
                    break;
            }
        } catch (UnsupportedVersionException ex) {
            // Ignore unsupported versions during replay
        }
    }

    @Override
    public void onLoaded(CoordinatorMetadataImage newImage) {
        log.info("Loaded shard for {}.", topicPartition);
        metadataManagerBridge.onShardLoaded();
    }

    @Override
    public void onUnloaded() {
        metadataManagerBridge.onShardUnloaded(topicPartition.partition(), numPartitions);
        log.info("Unloaded shard for {}.", topicPartition);
    }

    @Override
    public void onNewMetadataImage(CoordinatorMetadataImage newImage, CoordinatorMetadataDelta delta) {
    }

    // ---------------------------------------------------------------
    // Write operations (return CoordinatorResult)
    // ---------------------------------------------------------------

    public CoordinatorResult<Void, CoordinatorRecord> transitionTo(
        String mirrorName,
        TopicPartition tp,
        MirrorPartitionState newState,
        String errorMessage,
        boolean nonRetryable
    ) {
        MirrorPartitionState currentState = metadataManagerBridge.getPartitionState(mirrorName, tp);
        if (!MirrorPartitionState.isValidTransition(currentState, newState)) {
            log.warn("Skipping invalid transition from {} to {} for partition {}.", currentState, newState, tp);
            return new CoordinatorResult<>(List.of(), null);
        }

        log.debug("Transitioning partition {} from {} to {}.", tp, currentState, newState);
        updateFailedState(mirrorName, tp, currentState, newState, errorMessage, nonRetryable);
        CoordinatorRecord record = buildPartitionStateRecord(mirrorName, tp, newState);
        return new CoordinatorResult<>(List.of(record), null);
    }

    public CoordinatorResult<Void, CoordinatorRecord> updateLastMirrorEpoch(
        String mirrorName, TopicPartition tp, int epoch
    ) {
        MirrorPartitionKey pk = MirrorPartitionKey.of(
            mirrorName, metadataManagerBridge.getTopicId(tp.topic()), tp.partition());
        CoordinatorRecord record = buildLastMirrorEpochsRecord(pk, epoch);
        return new CoordinatorResult<>(List.of(record), null);
    }

    public CoordinatorResult<Void, CoordinatorRecord> tombstoneMirrorRecords(
        String mirrorName,
        Set<TopicPartition> partitions
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            Uuid topicId = metadataManagerBridge.getTopicId(tp.topic());
            records.add(CoordinatorRecord.tombstone(new MirrorPartitionStateKey()
                .setMirrorName(mirrorName).setTopicId(topicId).setPartition(tp.partition())));
            records.add(CoordinatorRecord.tombstone(new LastMirrorEpochsKey()
                .setMirrorName(mirrorName).setTopicId(topicId).setPartition(tp.partition())));
        }
        return new CoordinatorResult<>(records, null);
    }

    // ---------------------------------------------------------------
    // Private replay helpers
    // ---------------------------------------------------------------

    private void replayPartitionState(MirrorPartitionStateKey key, ApiMessageAndVersion value) {
        MirrorPartitionKey pk = MirrorPartitionKey.of(key.mirrorName(), key.topicId(), key.partition());
        if (value != null) {
            MirrorPartitionStateValue stateValue = (MirrorPartitionStateValue) value.message();
            MirrorPartitionState state = MirrorPartitionState.fromValue(stateValue.state());
            MirrorPartitionState previousState = MirrorPartitionState.fromValue(stateValue.previousState());
            metadataManagerBridge.setPartitionState(pk, state);
            restoreFailedState(pk, state, stateValue.retryAttempt(), stateValue.errorMessage(), previousState);
        } else {
            metadataManagerBridge.clearPartitionState(pk);
        }
    }

    private void replayLastMirrorEpochs(LastMirrorEpochsKey key, ApiMessageAndVersion value) {
        MirrorPartitionKey pk = MirrorPartitionKey.of(key.mirrorName(), key.topicId(), key.partition());
        if (value != null) {
            LastMirrorEpochsValue epochsValue = (LastMirrorEpochsValue) value.message();
            metadataManagerBridge.getTopicName(key.topicId()).ifPresent(topicName ->
                metadataManagerBridge.setLastMirrorEpoch(key.mirrorName(),
                    topicName, key.partition(), epochsValue.lastMirrorEpoch()));
        } else {
            metadataManagerBridge.clearPartitionState(pk);
        }
    }

    // ---------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------

    private void updateFailedState(String mirrorName, TopicPartition tp,
                                   MirrorPartitionState currentState, MirrorPartitionState newState,
                                   String errorMessage, boolean nonRetryable) {
        MirrorPartitionKey pk = MirrorPartitionKey.of(
            mirrorName, metadataManagerBridge.getTopicId(tp.topic()), tp.partition());
        metadataManagerBridge.updateFailedInfo(pk, currentState, newState, errorMessage, nonRetryable);
    }

    private void restoreFailedState(MirrorPartitionKey pk, MirrorPartitionState state,
                                    int retryAttempt, String errorMessage, MirrorPartitionState previousState) {
        if (state == MirrorPartitionState.FAILED) {
            metadataManagerBridge.setFailedInfo(pk, MirrorPartition.EMPTY.withError(errorMessage, retryAttempt, previousState));
        } else if (state == MirrorPartitionState.LOG_TRUNCATION
                || state == MirrorPartitionState.STOPPED
                || state == MirrorPartitionState.PAUSED) {
            metadataManagerBridge.clearFailedInfo(pk);
        }
    }

    private CoordinatorRecord buildPartitionStateRecord(String mirrorName, TopicPartition tp,
                                                        MirrorPartitionState newState) {
        MirrorPartitionKey pk = MirrorPartitionKey.of(
            mirrorName, metadataManagerBridge.getTopicId(tp.topic()), tp.partition());
        MirrorPartition mp = metadataManagerBridge.getFailedInfo(pk);
        var key = new MirrorPartitionStateKey()
            .setMirrorName(mirrorName)
            .setTopicId(pk.topicId())
            .setPartition(pk.partition());
        var val = new MirrorPartitionStateValue()
            .setState(newState.value())
            .setPreviousState(mp != null && mp.prevState() != null ? mp.prevState().value() : MirrorPartitionState.UNKNOWN.value())
            .setRetryAttempt(mp != null ? (short) mp.retryAttempt() : (short) 0)
            .setErrorMessage(mp != null ? mp.errorMessage() : null);
        return CoordinatorRecord.record(key, new ApiMessageAndVersion(val, MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION));
    }

    private CoordinatorRecord buildLastMirrorEpochsRecord(MirrorPartitionKey pk, int lastMirrorEpoch) {
        var key = new LastMirrorEpochsKey()
            .setMirrorName(pk.mirrorName())
            .setTopicId(pk.topicId())
            .setPartition(pk.partition());
        var val = new LastMirrorEpochsValue().setLastMirrorEpoch(lastMirrorEpoch);
        return CoordinatorRecord.record(key, new ApiMessageAndVersion(val, LastMirrorEpochsValue.HIGHEST_SUPPORTED_VERSION));
    }
}
