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
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
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
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Persistence layer for the cluster mirror coordinator.
 * Implements the {@link ClusterMirrorCoordinator} lifecycle interface and
 * delegates record persistence to the {@link CoordinatorRuntime}.
 * Exposes shard write operations via {@link CoreBridge.CoordinatorWriter}
 * so that {@code MirrorMetadataManager} can trigger side effects after
 * writes commit.
 */
public class ClusterMirrorCoordinatorService implements ClusterMirrorCoordinator {
    private final Logger log;
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final ClusterMirrorConfig config;
    private final CoordinatorRuntime<ClusterMirrorCoordinatorShard, CoordinatorRecord> runtime;
    private final CoreBridge bridge;
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
        private CoreBridge bridge;
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

        public Builder withBridge(CoreBridge bridge) {
            this.bridge = bridge;
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
                            () -> new ClusterMirrorCoordinatorShard.Builder(bridge, numPartitions))
                    .withDefaultWriteTimeOut(Duration.ofMillis(config.coordinatorWriteTimeoutMs()))
                    .withCoordinatorRuntimeMetrics(runtimeMetrics)
                    .withCoordinatorMetrics(new ClusterMirrorCoordinatorMetrics())
                    .withSerializer(new MirrorRecordSerde())
                    .withAppendLingerMs(config.coordinatorAppendLingerMs())
                    .withExecutorService(Executors.newSingleThreadExecutor())
                    .build();

            return new ClusterMirrorCoordinatorService(
                    nodeId, config, runtime, bridge,
                    scheduler, metrics);
        }
    }

    ClusterMirrorCoordinatorService(
        int nodeId,
        ClusterMirrorConfig config,
        CoordinatorRuntime<ClusterMirrorCoordinatorShard, CoordinatorRecord> runtime,
        CoreBridge bridge,
        Scheduler scheduler,
        Metrics metrics
    ) {
        String name = "[ClusterMirrorCoordinatorService id=" + nodeId + "] ";
        this.log = new LogContext(name).logger(ClusterMirrorCoordinatorService.class);
        this.config = config;
        this.runtime = runtime;
        this.bridge = bridge;
        this.scheduler = scheduler;
        this.metrics = metrics;
    }

    @Override
    public void startup() {
        if (!isActive.compareAndSet(false, true)) {
            log.warn("Is already running.");
            return;
        }
        log.info("Starting up.");
        bridge.initialize(
            new CoreBridge.CoordinatorWriter() {
                @Override
                public CompletableFuture<Void> writePartitionState(String mirrorName, TopicPartition tp,
                        MirrorPartitionState state, int stateEpoch, String errorMessage, boolean nonRetryable) {
                    return ClusterMirrorCoordinatorService.this.writePartitionState(
                        mirrorName, tp, state, stateEpoch, errorMessage, nonRetryable);
                }

                @Override
                public CompletableFuture<Void> writeLastMirrorEpoch(String mirrorName,
                        TopicPartition tp, int epoch) {
                    return ClusterMirrorCoordinatorService.this.writeLastMirrorEpoch(
                        mirrorName, tp, epoch);
                }

                @Override
                public CompletableFuture<Void> writeTombstone(String mirrorName,
                        Set<TopicPartition> partitions) {
                    return ClusterMirrorCoordinatorService.this.writeTombstone(
                        mirrorName, partitions);
                }
            },
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
        bridge.closeSourceAdmins();
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

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }

    public int partitionFor(MirrorPartitionKey key) {
        throwIfNotActive();
        return key.coordinatorPartition(config.stateTopicNumPartitions());
    }

    /**
     * Reads partition states from the local coordinator cache. Called from
     * {@code MirrorMetadataManager#readStatesFromRemoteCoordinator} of a
     * remote broker.
     */
    public void readState(
            String mirrorName,
            Map<String, Set<Integer>> partitions,
            Consumer<ReadMirrorStatesResponse> callback
    ) {
        throwIfNotActive();

        Map<Integer, Map<String, Set<Integer>>> byCoordPartition = new HashMap<>();
        partitions.forEach((topic, parts) -> parts.forEach(part -> {
            int cp = partitionFor(MirrorPartitionKey.of(mirrorName, bridge.getTopicId(topic), part));
            byCoordPartition.computeIfAbsent(cp, k -> new HashMap<>())
                .computeIfAbsent(topic, k -> new HashSet<>()).add(part);
        }));

        List<CompletableFuture<ReadMirrorStatesResponseData>> futures = new ArrayList<>();
        byCoordPartition.forEach((cp, tps) -> {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, cp);
            futures.add(runtime.scheduleReadOperation("read-mirror-states", mirrorStateTp,
                (shard, offset) -> shard.readState(mirrorName, tps)));
        });

        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .whenComplete((v, e) -> {
                ReadMirrorStatesResponseData data = new ReadMirrorStatesResponseData();
                if (e != null) {
                    log.error("Failed to read mirror states for {}", mirrorName, e);
                    data.setErrorCode(Errors.forException(e).code());
                    data.setErrorMessage(e.getMessage());
                } else {
                    // A topic's partitions can span multiple coordinator partitions, so merge results by topic name here
                    Map<String, List<ReadMirrorStatesResponseData.PartitionResult>> merged = new HashMap<>();
                    futures.forEach(f -> f.join().topics().forEach(topicResult ->
                        merged.computeIfAbsent(topicResult.name(), k -> new ArrayList<>())
                            .addAll(topicResult.partitions())));
                    List<ReadMirrorStatesResponseData.TopicResult> topicResults = merged.entrySet().stream()
                        .map(topicEntry -> new ReadMirrorStatesResponseData.TopicResult()
                            .setName(topicEntry.getKey())
                            .setPartitions(topicEntry.getValue()))
                        .toList();
                    data.setTopics(topicResults);
                }
                callback.accept(new ReadMirrorStatesResponse(data));
            });
    }

    /**
     * Writes partition states and last mirror epochs received from a remote broker.
     * Called from {@code MirrorMetadataManager#writeStatesToRemoteCoordinator} of
     * a remote broker.
     */
    public void writeState(
            String mirrorName,
            Map<String, Set<MirrorStateWrite>> mirrorStates,
            Consumer<WriteMirrorStatesResponse> callback
    ) {
        throwIfNotActive();

        Map<Integer, Map<String, Set<MirrorStateWrite>>> byCoordPartition = new HashMap<>();
        Map<String, Set<Integer>> tps = new HashMap<>();

        mirrorStates.forEach((topic, partitions) -> {
            Set<Integer> partitionIndices = new HashSet<>();
            partitions.forEach(partition -> {
                partitionIndices.add(partition.partition());
                int cp = partitionFor(MirrorPartitionKey.of(
                    mirrorName, bridge.getTopicId(topic), partition.partition()));
                byCoordPartition.computeIfAbsent(cp, k -> new HashMap<>())
                    .computeIfAbsent(topic, k -> new HashSet<>()).add(partition);
            });
            tps.put(topic, partitionIndices);
        });

        List<CompletableFuture<Map<TopicPartition, ClusterMirrorCoordinatorShard.PartitionWriteResult>>> futures = new ArrayList<>();
        byCoordPartition.forEach((cp, states) -> {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, cp);
            futures.add(runtime.scheduleWriteOperation("write-states", mirrorStateTp,
                Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                shard -> shard.writeState(mirrorName, states)));
        });

        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .whenComplete((v, e) -> {
                WriteMirrorStatesResponseData data = new WriteMirrorStatesResponseData();
                if (e != null) {
                    log.error("Failed to write mirror states for {}", mirrorName, e);
                    data.setErrorCode(Errors.forException(e).code());
                    data.setErrorMessage(e.getMessage());
                } else {
                    Map<TopicPartition, ClusterMirrorCoordinatorShard.PartitionWriteResult> allResults = new HashMap<>();
                    futures.forEach(f -> allResults.putAll(f.join()));

                    List<WriteMirrorStatesResponseData.TopicResult> topicResults = new ArrayList<>();
                    tps.forEach((topic, indices) -> {
                        List<WriteMirrorStatesResponseData.PartitionResult> partitionResults = new ArrayList<>();
                        indices.forEach(i -> {
                            TopicPartition tp = new TopicPartition(topic, i);
                            ClusterMirrorCoordinatorShard.PartitionWriteResult pwr = allResults.get(tp);
                            WriteMirrorStatesResponseData.PartitionResult pr =
                                    new WriteMirrorStatesResponseData.PartitionResult();
                            pr.setPartitionIndex(i);
                            if (pwr != null) {
                                pr.setStateEpoch(pwr.stateEpoch());
                                pr.setErrorCode(pwr.error().code());
                            }
                            partitionResults.add(pr);
                        });
                        topicResults.add(new WriteMirrorStatesResponseData.TopicResult()
                            .setName(topic).setPartitions(partitionResults));
                    });
                    data.setTopics(topicResults);
                }
                callback.accept(new WriteMirrorStatesResponse(data));
            });
    }

    /** Persists a partition state record. Called from {@code MirrorMetadataManager#transitionTo}. */
    private CompletableFuture<Void> writePartitionState(
            String mirrorName, TopicPartition tp, MirrorPartitionState state,
            int stateEpoch, String errorMessage, boolean nonRetryable
    ) {
        throwIfNotActive();
        TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME,
                partitionFor(MirrorPartitionKey.of(mirrorName, bridge.getTopicId(tp.topic()), tp.partition())));
        return runtime.scheduleWriteOperation("write-partition-state", mirrorStateTp,
                Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                shard -> shard.writePartitionState(mirrorName, tp, state, stateEpoch, errorMessage, nonRetryable));
    }

    /** Persists a last mirror epoch record. Called from {@code MirrorMetadataManager#updateLastMirrorEpoch}. */
    private CompletableFuture<Void> writeLastMirrorEpoch(
            String mirrorName, TopicPartition tp, int epoch
    ) {
        throwIfNotActive();
        TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME,
                partitionFor(MirrorPartitionKey.of(mirrorName, bridge.getTopicId(tp.topic()), tp.partition())));
        return runtime.scheduleWriteOperation("write-lme", mirrorStateTp,
                Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                shard -> shard.writeLastMirrorEpoch(mirrorName, tp, epoch));
    }

    /** Persists tombstone records for a deleted mirror. Called from {@code MirrorMetadataManager#tombstoneMirror}. */
    private CompletableFuture<Void> writeTombstone(
            String mirrorName, Set<TopicPartition> partitions
    ) {
        throwIfNotActive();
        int coordPartition = partitionFor(MirrorPartitionKey.of(
                mirrorName, bridge.getTopicId(partitions.iterator().next().topic()),
                partitions.iterator().next().partition()));
        TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, coordPartition);
        return runtime.scheduleWriteOperation("write-tombstone", mirrorStateTp,
                Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                shard -> shard.writeTombstone(mirrorName, partitions));
    }

    /** A single partition state or LME write entry for inter-broker WriteMirrorStates RPCs. */
    public record MirrorStateWrite(int partition, MirrorPartitionState state, int stateEpoch, Integer leaderEpoch) { }
}
