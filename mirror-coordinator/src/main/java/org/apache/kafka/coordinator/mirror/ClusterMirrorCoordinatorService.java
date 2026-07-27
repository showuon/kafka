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
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.message.DeleteClusterMirrorRequestData;
import org.apache.kafka.common.message.PauseMirrorTopicsRequestData;
import org.apache.kafka.common.message.ResumeMirrorTopicsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.common.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.mirror.bridge.MirrorMetadataManagerServiceBridge;
import org.apache.kafka.coordinator.mirror.bridge.MirrorMetadataManagerShardBridge;
import org.apache.kafka.coordinator.mirror.metrics.ClusterMirrorCoordinatorMetrics;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.server.util.timer.Timer;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Service layer for the cluster mirror coordinator.
 * Implements the {@link ClusterMirrorCoordinator} lifecycle interface and
 * delegates record persistence to the {@link CoordinatorRuntime}.
 * Side effects (fetcher management, epoch bumps, truncation) are triggered
 * after writes commit (via {@code .whenComplete} on the runtime futures).
 */
public class ClusterMirrorCoordinatorService implements ClusterMirrorCoordinator {
    private final Logger log;
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final int nodeId;
    private final ClusterMirrorConfig config;
    private final CoordinatorRuntime<ClusterMirrorCoordinatorShard, CoordinatorRecord> runtime;
    private final MirrorMetadataManagerServiceBridge serviceBridge;
    private final Scheduler scheduler;
    private final Metrics metrics;

    public static class Builder {
        private final int nodeId;
        private final ClusterMirrorConfig config;
        private PartitionWriter writer;
        private CoordinatorLoader<CoordinatorRecord> loader;
        private Time time;
        private Timer timer;
        private CoordinatorRuntimeMetrics runtimeMetrics;
        private MirrorMetadataManagerServiceBridge serviceBridge;
        private MirrorMetadataManagerShardBridge shardBridge;
        private Scheduler scheduler;
        private Metrics metrics;

        public Builder(int nodeId, ClusterMirrorConfig config) {
            this.nodeId = nodeId;
            this.config = config;
        }

        public Builder withWriter(PartitionWriter writer) {
            this.writer = writer;
            return this;
        }

        public Builder withLoader(CoordinatorLoader<CoordinatorRecord> loader) {
            this.loader = loader;
            return this;
        }

        public Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public Builder withCoordinatorRuntimeMetrics(CoordinatorRuntimeMetrics runtimeMetrics) {
            this.runtimeMetrics = runtimeMetrics;
            return this;
        }

        public Builder withServiceBridge(MirrorMetadataManagerServiceBridge serviceBridge) {
            this.serviceBridge = serviceBridge;
            return this;
        }

        public Builder withShardBridge(MirrorMetadataManagerShardBridge shardBridge) {
            this.shardBridge = shardBridge;
            return this;
        }

        public Builder withScheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder withMetrics(Metrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public ClusterMirrorCoordinatorService build() {
            int numPartitions = config.stateTopicNumPartitions();

            var logContext = new LogContext("[ClusterMirrorCoordinator id=" + nodeId + "] ");
            var eventProcessor = new MultiThreadedEventProcessor(
                    logContext,
                    "cluster-mirror-coordinator-event-processor-",
                    config.coordinatorNumThreads(),
                    time,
                    runtimeMetrics);

            var runtime = new CoordinatorRuntime.Builder<ClusterMirrorCoordinatorShard, CoordinatorRecord>()
                    .withTime(time)
                    .withTimer(timer)
                    .withEventProcessor(eventProcessor)
                    .withPartitionWriter(writer)
                    .withLoader(loader)
                    .withCoordinatorShardBuilderSupplier(
                            () -> new ClusterMirrorCoordinatorShard.Builder(shardBridge, numPartitions))
                    .withDefaultWriteTimeOut(Duration.ofMillis(config.coordinatorWriteTimeoutMs()))
                    .withCoordinatorRuntimeMetrics(runtimeMetrics)
                    .withCoordinatorMetrics(new ClusterMirrorCoordinatorMetrics())
                    .withSerializer(new ClusterMirrorRecordSerde())
                    .withAppendLingerMs(config.coordinatorAppendLingerMs())
                    .withExecutorService(Executors.newSingleThreadExecutor())
                    .build();

            return new ClusterMirrorCoordinatorService(
                    nodeId, config, runtime, serviceBridge, scheduler, metrics);
        }
    }

    ClusterMirrorCoordinatorService(
        int nodeId,
        ClusterMirrorConfig config,
        CoordinatorRuntime<ClusterMirrorCoordinatorShard, CoordinatorRecord> runtime,
        MirrorMetadataManagerServiceBridge serviceBridge,
        Scheduler scheduler,
        Metrics metrics
    ) {
        String name = "[ClusterMirrorCoordinatorService id=" + nodeId + "] ";
        this.log = new LogContext(name).logger(ClusterMirrorCoordinatorService.class);
        this.nodeId = nodeId;
        this.config = config;
        this.runtime = runtime;
        this.serviceBridge = serviceBridge;
        this.scheduler = scheduler;
        this.metrics = metrics;
    }

    // ---------------------------------------------------------------
    // Lifecycle (ClusterMirrorCoordinator interface)
    // ---------------------------------------------------------------

    @Override
    public void startup() {
        if (!isActive.compareAndSet(false, true)) {
            log.warn("Is already running.");
            return;
        }
        log.info("Starting up.");
        serviceBridge.initialize(
            this::transitionTo,
            this::updateLastMirrorEpoch,
            this::tombstoneMirror,
            this::partitionFor,
            this::partitionFor);
        log.info("Startup complete.");
    }

    @Override
    public void shutdown() {
        if (!isActive.compareAndSet(true, false)) {
            log.warn("Is already shutting down.");
            return;
        }
        log.info("Shutting down.");
        serviceBridge.closeSourceAdmins();
        try {
            scheduler.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while shutting down scheduler", e);
        }
        Utils.closeQuietly(runtime, "coordinator runtime");
        Utils.closeQuietly(metrics, "coordinator metrics");
        log.info("Shutdown complete.");
    }

    @Override
    public void onElection(int partitionIndex, int partitionLeaderEpoch) {
        TopicPartition tp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, partitionIndex);
        runtime.scheduleLoadOperation(tp, partitionLeaderEpoch);
    }

    @Override
    public void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch) {
        TopicPartition tp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, partitionIndex);
        runtime.scheduleUnloadOperation(tp, partitionLeaderEpoch);
    }

    @Override
    public void onNewMetadataImage(CoordinatorMetadataImage newImage, CoordinatorMetadataDelta delta) {
        runtime.onNewMetadataImage(newImage, delta);
    }

    // ---------------------------------------------------------------
    // Partition routing
    // ---------------------------------------------------------------

    /** Returns the __mirror_state partition index for the given mirror name. */
    private int partitionFor(String mirrorName) {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
        return Utils.abs(mirrorName.hashCode()) % config.stateTopicNumPartitions();
    }

    /** Returns the __mirror_state partition index for the given partition key. */
    public int partitionFor(ClusterMirrorPartitionKey key) {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
        return Utils.abs(key.asCoordinatorKey().hashCode()) % config.stateTopicNumPartitions();
    }

    /** Returns true if this broker leads the __mirror_state partition for the given mirror partition. */
    private boolean isLocal(String mirrorName, TopicPartition tp) {
        int partition = partitionFor(ClusterMirrorPartitionKey.of(
                mirrorName, serviceBridge.getTopicId(tp.topic()), tp.partition()));
        int leader = serviceBridge.getLeaderForPartition(MIRROR_STATE_TOPIC_NAME, partition);
        return leader == nodeId;
    }

    // ---------------------------------------------------------------
    // State transitions
    // ---------------------------------------------------------------


//    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions,
//                             MirrorPartitionState newState, String errorMessage, boolean nonRetryable) {
//        topicPartitions.forEach(tp -> {
//            if (isLocal(mirrorName, tp)) {
//                TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME,
//                    partitionFor(ClusterMirrorPartitionKey.of(mirrorName, serviceBridge.getTopicId(tp.topic()), tp.partition())));
//                runtime.scheduleWriteOperation("transition-" + newState, mirrorStateTp,
//                        Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
//                        shard -> shard.transitionTo(mirrorName, tp, newState, errorMessage, nonRetryable))
//                    .whenComplete((result, ex) -> {
//                        if (ex != null) {
//                            Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
//                                ? ex.getCause() : ex;
//                            if (cause instanceof CoordinatorLoadInProgressException) {
//                                log.debug("Transition to {} deferred for {} (shard loading).", newState, tp);
//                                return;
//                            }
//                            log.error("Transition to {} failed for {}", newState, tp, ex);
//                            if (newState != MirrorPartitionState.FAILED) {
//                                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage());
//                            }
//                            return;
//                        }
//                        ClusterMirrorPartitionKey key = ClusterMirrorPartitionKey.of(
//                            mirrorName, serviceBridge.getTopicId(tp.topic()), tp.partition());
//                        MirrorPartitionState currentState = serviceBridge.getPartitionState(
//                            key.mirrorName(), tp);
//                        if (currentState == newState) {
//                            serviceBridge.handleSideEffect(mirrorName, tp, newState);
//                        }
//                    });
//            } else {
//                Map<String, Set<PartitionStateInfo>> topicMetadata =
//                    Map.of(tp.topic(), Set.of(new PartitionStateInfo(tp.partition(), newState, -1)));
//                serviceBridge.writeStatesToRemoteCoordinator(mirrorName, topicMetadata, Set.of(),
//                    res -> res.data().topics().forEach(topic -> topic.partitions().forEach(par -> {
//                        if (par.errorCode() == Errors.NONE.code()) {
//                            ClusterMirrorPartitionKey key = ClusterMirrorPartitionKey.of(mirrorName,
//                                serviceBridge.getTopicId(tp.topic()), tp.partition());
//                            updateLocalFailedState(key, newState, errorMessage, nonRetryable);
//                            serviceBridge.setPartitionState(key, newState);
//                            serviceBridge.handleSideEffect(mirrorName, tp, newState);
//                        } else {
//                            log.error("Failed to write partition state to remote coordinator: {}", par.errorCode());
//                        }
//                    })));
//            }
//        });
//    }
//
//    private void updateLocalFailedState(ClusterMirrorPartitionKey key,
//                                        MirrorPartitionState newState,
//                                        String errorMessage, boolean nonRetryable) {
//        MirrorPartitionState curState = serviceBridge.getPartitionState(
//                key.mirrorName(), new TopicPartition(
//                        serviceBridge.getTopicName(key.topicId()).orElse(""), key.partition()));
//        serviceBridge.updateFailedState(key, curState, newState, errorMessage, nonRetryable);
//    }

    // ---------------------------------------------------------------
    // LME operations
    // ---------------------------------------------------------------

    public CompletableFuture<Void> updateLastMirrorEpoch(String mirrorName, TopicPartition tp, int epoch) {
        if (epoch == -1) {
            return CompletableFuture.completedFuture(null);
        }
        serviceBridge.setLastMirrorEpoch(mirrorName, tp.topic(), tp.partition(), epoch);

        if (isLocal(mirrorName, tp)) {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME,
                partitionFor(ClusterMirrorPartitionKey.of(mirrorName, serviceBridge.getTopicId(tp.topic()), tp.partition())));
            return runtime.scheduleWriteOperation("update-lme", mirrorStateTp,
                Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                shard -> shard.updateLastMirrorEpoch(mirrorName, tp, epoch));
        } else {
            serviceBridge.writeStatesToRemoteCoordinator(mirrorName,
                Map.of(tp.topic(), Set.of(new PartitionStateInfo(tp.partition(), null, epoch))),
                Set.of(), res -> { });
            return CompletableFuture.completedFuture(null);
        }
    }

    // ---------------------------------------------------------------
    // Private: tombstones
    // ---------------------------------------------------------------

    private void tombstoneMirror(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> states = serviceBridge.getMirrorStates(mirrorName);
        Map<Integer, Set<TopicPartition>> coordPartitionToMirrorPartitions = new HashMap<>();
        states.forEach((tp, state) -> {
            if (isLocal(mirrorName, tp)) {
                coordPartitionToMirrorPartitions.computeIfAbsent(
                        partitionFor(ClusterMirrorPartitionKey.of(mirrorName, serviceBridge.getTopicId(tp.topic()), tp.partition())),
                        v -> new HashSet<>()).add(tp);
            }
        });

        serviceBridge.removeCachedMirror(mirrorName);
        serviceBridge.removeStateForPartitions(states.keySet());

        if (coordPartitionToMirrorPartitions.isEmpty()) {
            states.keySet().forEach(tp ->
                    serviceBridge.clearPartitionState(
                            ClusterMirrorPartitionKey.of(mirrorName, serviceBridge.getTopicId(tp.topic()), tp.partition())));
            return;
        }

        for (Map.Entry<Integer, Set<TopicPartition>> entry : coordPartitionToMirrorPartitions.entrySet()) {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, entry.getKey());
            Set<TopicPartition> tps = entry.getValue();
            runtime.scheduleWriteOperation("tombstone-mirror", mirrorStateTp,
                            Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                            shard -> shard.tombstoneMirrorRecords(mirrorName, tps))
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.warn("Failed to write tombstone to {}: {}. Will retry later.",
                                    mirrorStateTp, ex.getMessage());
                        } else {
                            tps.forEach(tp -> serviceBridge.clearPartitionState(
                                    ClusterMirrorPartitionKey.of(mirrorName,
                                            serviceBridge.getTopicId(tp.topic()), tp.partition())));
                        }
                    });
        }
    }

    public void scheduleMetadataRefresh(long intervalMs) {
        serviceBridge.scheduleMetadataRefresh(intervalMs);
    }
}
