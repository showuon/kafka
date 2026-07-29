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
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
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
import java.util.Map;
import java.util.Set;

/**
 * The shard (state machine) for the cluster mirror coordinator.
 * One instance per __mirror_state partition, managed by the CoordinatorRuntime.
 */
public class ClusterMirrorCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    private final Logger log;
    private final CoreBridge coreBridge;
    private final TopicPartition topicPartition;
    private final int numPartitions;

    public static class Builder implements CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> {
        private final CoreBridge coreBridge;
        private final int numPartitions;
        private LogContext logContext;
        private TopicPartition topicPartition;

        public Builder(CoreBridge coreBridge, int numPartitions) {
            this.coreBridge = coreBridge;
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
            return new ClusterMirrorCoordinatorShard(logContext, coreBridge, topicPartition, numPartitions);
        }
    }

    private ClusterMirrorCoordinatorShard(
        LogContext logContext,
        CoreBridge coreBridge,
        TopicPartition topicPartition,
        int numPartitions
    ) {
        this.log = logContext.logger(ClusterMirrorCoordinatorShard.class);
        this.coreBridge = coreBridge;
        this.topicPartition = topicPartition;
        this.numPartitions = numPartitions;
    }

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

    private void replayPartitionState(MirrorPartitionStateKey key, ApiMessageAndVersion value) {
        MirrorPartitionKey pk = MirrorPartitionKey.of(key.mirrorName(), key.topicId(), key.partition());
        if (value != null) {
            MirrorPartitionStateValue stateValue = (MirrorPartitionStateValue) value.message();
            MirrorPartitionState state = MirrorPartitionState.fromValue(stateValue.state());
            MirrorPartitionState previousState = MirrorPartitionState.fromValue(stateValue.previousState());
            MirrorPartition mp = MirrorPartition.orEmpty(coreBridge.getPartition(pk)).withState(state);
            if (state == MirrorPartitionState.FAILED) {
                mp = mp.withError(stateValue.errorMessage(), stateValue.retryAttempt(), previousState);
            } else if (state == MirrorPartitionState.LOG_TRUNCATION
                    || state == MirrorPartitionState.STOPPED
                    || state == MirrorPartitionState.PAUSED) {
                mp = mp.clearError();
            }
            coreBridge.setPartition(pk, mp);
        } else {
            coreBridge.removePartition(pk);
        }
    }

    private void replayLastMirrorEpochs(LastMirrorEpochsKey key, ApiMessageAndVersion value) {
        MirrorPartitionKey pk = MirrorPartitionKey.of(key.mirrorName(), key.topicId(), key.partition());
        if (value != null) {
            LastMirrorEpochsValue epochsValue = (LastMirrorEpochsValue) value.message();
            coreBridge.getTopicName(key.topicId()).ifPresent(topicName ->
                    coreBridge.setLastMirrorEpoch(key.mirrorName(),
                            topicName, key.partition(), epochsValue.lastMirrorEpoch()));
        } else {
            coreBridge.removePartition(pk);
        }
    }

    @Override
    public void onLoaded(CoordinatorMetadataImage newImage) {
        log.info("Loaded shard for {}.", topicPartition);
        coreBridge.onShardLoaded();
    }

    @Override
    public void onUnloaded() {
        coreBridge.onShardUnloaded(topicPartition.partition(), numPartitions);
        log.info("Unloaded shard for {}.", topicPartition);
    }

    @Override
    public void onNewMetadataImage(CoordinatorMetadataImage newImage, CoordinatorMetadataDelta delta) {
    }

    public ReadMirrorStatesResponseData readState(
            String mirrorName, Map<String, Set<Integer>> partitions
    ) {
        ReadMirrorStatesResponseData data = new ReadMirrorStatesResponseData();
        List<ReadMirrorStatesResponseData.TopicResult> topicResults = new ArrayList<>();
        partitions.forEach((topic, parts) -> {
            List<ReadMirrorStatesResponseData.PartitionResult> partitionResults = new ArrayList<>();
            parts.forEach(part -> {
                MirrorPartitionKey pk = MirrorPartitionKey.of(mirrorName, coreBridge.getTopicId(topic), part);
                MirrorPartition mp = MirrorPartition.orEmpty(coreBridge.getPartition(pk));
                ReadMirrorStatesResponseData.PartitionResult pr = new ReadMirrorStatesResponseData.PartitionResult()
                        .setPartitionIndex(part)
                        .setState(mp.state().value())
                        .setPreviousState(mp.prevState() != null ? mp.prevState().value() : MirrorPartitionState.UNKNOWN.value())
                        .setLastMirrorEpoch(mp.lastMirrorEpoch())
                        .setRetryAttempt((short) mp.retryAttempt())
                        .setErrorMessage(mp.errorMessage());
                partitionResults.add(pr);
            });
            topicResults.add(new ReadMirrorStatesResponseData.TopicResult()
                    .setName(topic).setPartitions(partitionResults));
        });
        data.setTopics(topicResults);
        return data;
    }

    public CoordinatorResult<Void, CoordinatorRecord> writeState(
        String mirrorName,
        Map<String, Set<ClusterMirrorCoordinatorService.MirrorStateWrite>> mirrorStates
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();
        mirrorStates.forEach((topic, partitions) -> partitions.forEach(partition -> {
            TopicPartition tp = new TopicPartition(topic, partition.partition());
            if (partition.state() != null && partition.state() != MirrorPartitionState.UNKNOWN) {
                records.addAll(writePartitionState(mirrorName, tp, partition.state(), null, false).records());
            }
            if (partition.leaderEpoch() != -1) {
                records.addAll(writeLastMirrorEpoch(mirrorName, tp, partition.leaderEpoch()).records());
            }
        }));
        return new CoordinatorResult<>(records, null);
    }

    public CoordinatorResult<Void, CoordinatorRecord> writePartitionState(
            String mirrorName, TopicPartition tp, MirrorPartitionState state,
            String errorMessage, boolean nonRetryable
    ) {
        MirrorPartitionKey pk = MirrorPartitionKey.of(mirrorName, coreBridge.getTopicId(tp.topic()), tp.partition());
        MirrorPartitionState currentState = MirrorPartition.orEmpty(coreBridge.getPartition(pk)).state();
        if (!MirrorPartitionState.isValidTransition(currentState, state)) {
            log.warn("Skipping invalid transition from {} to {} for {}.", currentState, state, tp);
            return new CoordinatorResult<>(List.of(), null);
        }
        log.debug("Transitioning partition {} from {} to {}.", tp, currentState, state);
        coreBridge.updateFailedInfo(pk, currentState, state, errorMessage, nonRetryable);

        MirrorPartition mp = MirrorPartition.orEmpty(coreBridge.getPartition(pk));
        var key = new MirrorPartitionStateKey()
                .setMirrorName(mirrorName)
                .setTopicId(pk.topicId())
                .setPartition(pk.partition());
        var val = new MirrorPartitionStateValue()
                .setState(state.value())
                .setPreviousState(mp.prevState() != null ? mp.prevState().value() : MirrorPartitionState.UNKNOWN.value())
                .setRetryAttempt((short) mp.retryAttempt())
                .setErrorMessage(mp.errorMessage());
        CoordinatorRecord record = CoordinatorRecord.record(key,
                new ApiMessageAndVersion(val, MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION));
        return new CoordinatorResult<>(List.of(record), null);
    }

    public CoordinatorResult<Void, CoordinatorRecord> writeLastMirrorEpoch(
            String mirrorName, TopicPartition tp, int epoch
    ) {
        MirrorPartitionKey pk = MirrorPartitionKey.of(
                mirrorName, coreBridge.getTopicId(tp.topic()), tp.partition());
        var key = new LastMirrorEpochsKey()
                .setMirrorName(pk.mirrorName())
                .setTopicId(pk.topicId())
                .setPartition(pk.partition());
        var val = new LastMirrorEpochsValue().setLastMirrorEpoch(epoch);
        CoordinatorRecord record = CoordinatorRecord.record(key,
                new ApiMessageAndVersion(val, LastMirrorEpochsValue.HIGHEST_SUPPORTED_VERSION));
        return new CoordinatorResult<>(List.of(record), null);
    }

    public CoordinatorResult<Void, CoordinatorRecord> writeTombstone(
        String mirrorName, Set<TopicPartition> partitions
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            Uuid topicId = coreBridge.getTopicId(tp.topic());
            records.add(CoordinatorRecord.tombstone(new MirrorPartitionStateKey()
                .setMirrorName(mirrorName).setTopicId(topicId).setPartition(tp.partition())));
            records.add(CoordinatorRecord.tombstone(new LastMirrorEpochsKey()
                .setMirrorName(mirrorName).setTopicId(topicId).setPartition(tp.partition())));
        }
        return new CoordinatorResult<>(records, null);
    }
}
