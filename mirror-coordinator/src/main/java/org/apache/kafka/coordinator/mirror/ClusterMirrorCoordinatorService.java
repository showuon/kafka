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


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
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
    private final CoreBridge bridge;
    private final Scheduler scheduler;
    private final Metrics metrics;
    private final Time time;

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
                    scheduler, metrics, time);
        }
    }

    ClusterMirrorCoordinatorService(
        int nodeId,
        ClusterMirrorConfig config,
        CoordinatorRuntime<ClusterMirrorCoordinatorShard, CoordinatorRecord> runtime,
        CoreBridge bridge,
        Scheduler scheduler,
        Metrics metrics,
        Time time
    ) {
        String name = "[ClusterMirrorCoordinatorService id=" + nodeId + "] ";
        this.log = new LogContext(name).logger(ClusterMirrorCoordinatorService.class);
        this.nodeId = nodeId;
        this.config = config;
        this.runtime = runtime;
        this.bridge = bridge;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.time = time;
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
        bridge.initialize(
            this::transitionTo,
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

    private int partitionFor(String mirrorName) {
        throwIfNotActive();
        return Utils.abs(mirrorName.hashCode()) % config.stateTopicNumPartitions();
    }

    public int partitionFor(MirrorPartitionKey key) {
        throwIfNotActive();
        return key.coordinatorPartition(config.stateTopicNumPartitions());
    }

    /** Returns true if this broker leads the __mirror_state partition for the given mirror partition. */
    private boolean isLocal(String mirrorName, TopicPartition tp) {
        int partition = partitionFor(MirrorPartitionKey.of(
                mirrorName, bridge.getTopicId(tp.topic()), tp.partition()));
        int leader = bridge.getLeaderForPartition(MIRROR_STATE_TOPIC_NAME, partition);
        return leader == nodeId;
    }

    // ---------------------------------------------------------------
    // State transitions
    // ---------------------------------------------------------------

    private void onStateTransition(String mirrorName, TopicPartition tp, MirrorPartitionState newState) {
        throwIfNotActive();
        switch (newState) {
            case LOG_TRUNCATION:
                log.info("Mirror '{}' transitioning {} to LOG_TRUNCATION.", mirrorName, tp);
                scheduleTruncation(mirrorName, tp);
                break;
            case EPOCH_FENCING:
                log.info("Mirror '{}' transitioning {} to EPOCH_FENCING.", mirrorName, tp);
                bridge.scheduleBumpLeaderEpoch(mirrorName, tp)
                        .whenComplete((v, ex) -> {
                            if (ex != null) {
                                log.error("Mirror '{}' failed to bump leader epoch for {}.", mirrorName, tp, ex);
                                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage());
                            } else {
                                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING);
                            }
                        });
                break;
            case MIRRORING:
                log.info("Mirror '{}' transitioning {} to MIRRORING.", mirrorName, tp);
                bridge.maybeCreateMirrorFetchers(mirrorName, Set.of(tp));
                break;
            case PAUSING:
                log.info("Mirror '{}' transitioning {} to PAUSING.", mirrorName, tp);
                bridge.removeFetcherForPartitions(Set.of(tp));
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED);
                break;
            case PAUSED:
                log.info("Mirror '{}' transitioning {} to PAUSED.", mirrorName, tp);
                break;
            case STOPPING:
                log.info("Mirror '{}' transitioning {} to STOPPING.", mirrorName, tp);
                bridge.removeFetcherForPartitions(Set.of(tp));
                int latestEpoch = bridge.getLatestEpoch(tp).orElse(-1);
                updateLastMirrorEpoch(mirrorName, tp, latestEpoch)
                        .thenCompose(v -> bumpLeaderEpoch(tp))
                        .thenCompose(v -> abortOngoingTransactions(tp))
                        .thenCompose(v -> writePidResetBarrier(mirrorName, tp))
                        .thenAccept(v -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPED))
                        .exceptionally(ex -> {
                            log.error("Mirror '{}' STOPPING transition failed for {}.", mirrorName, tp, ex);
                            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage());
                            return null;
                        });
                break;
            case STOPPED:
                log.info("Mirror '{}' transitioning {} to STOPPED.", mirrorName, tp);
                break;
            case FAILED:
                log.info("Mirror '{}' transitioning {} to FAILED.", mirrorName, tp);
                scheduleFailedRetry(mirrorName, tp);
                break;
            default:
                throw new IllegalArgumentException("Illegal state transition to " + newState);
        }
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions,
                             MirrorPartitionState newState) {
        transitionTo(mirrorName, topicPartitions, newState, null, false);
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions,
                             MirrorPartitionState newState, String errorMessage) {
        transitionTo(mirrorName, topicPartitions, newState, errorMessage, false);
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions,
                             MirrorPartitionState newState, String errorMessage, boolean nonRetryable) {
        throwIfNotActive();
        topicPartitions.forEach(tp -> {
            if (isLocal(mirrorName, tp)) {
                TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME,
                    partitionFor(MirrorPartitionKey.of(mirrorName, bridge.getTopicId(tp.topic()), tp.partition())));
                runtime.scheduleWriteOperation("transition-" + newState, mirrorStateTp,
                                Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                                shard -> shard.transitionTo(mirrorName, tp, newState, errorMessage, nonRetryable))
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                                ? ex.getCause() : ex;
                            if (cause instanceof CoordinatorLoadInProgressException) {
                                log.debug("Transition to {} deferred for {} (shard loading).", newState, tp);
                                return;
                            }
                            log.error("Transition to {} failed for {}", newState, tp, ex);
                            if (newState != MirrorPartitionState.FAILED) {
                                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage());
                            }
                            return;
                        }
                        MirrorPartitionKey key = MirrorPartitionKey.of(
                            mirrorName, bridge.getTopicId(tp.topic()), tp.partition());
                        if (MirrorPartition.orEmpty(bridge.getPartition(key)).state() == newState) {
                            onStateTransition(mirrorName, tp, newState);
                        }
                    });
            } else {
                Map<String, Set<MirrorStateWrite>> topicMetadata =
                    Map.of(tp.topic(), Set.of(new MirrorStateWrite(tp.partition(), newState, -1)));
                bridge.writeStatesToRemoteCoordinator(mirrorName, topicMetadata, Set.of(),
                    res -> res.data().topics().forEach(topic -> topic.partitions().forEach(par -> {
                        if (par.errorCode() == Errors.NONE.code()) {
                            MirrorPartitionKey key = MirrorPartitionKey.of(mirrorName,
                                bridge.getTopicId(tp.topic()), tp.partition());
                            updateLocalFailedState(key, newState, errorMessage, nonRetryable);
                            bridge.setPartition(key, MirrorPartition.orEmpty(bridge.getPartition(key)).withState(newState));
                            onStateTransition(mirrorName, tp, newState);
                        } else {
                            log.error("Failed to write partition state to remote coordinator: {}", par.errorCode());
                        }
                    })));
            }
        });
    }

    private void updateLocalFailedState(MirrorPartitionKey key,
                                        MirrorPartitionState newState,
                                        String errorMessage, boolean nonRetryable) {
        MirrorPartitionState curState = MirrorPartition.orEmpty(bridge.getPartition(key)).state();
        bridge.updateFailedInfo(key, curState, newState, errorMessage, nonRetryable);
    }

    // ---------------------------------------------------------------
    // Inter-broker RPC handling
    // ---------------------------------------------------------------

    /**
     * Reads partition states from the local coordinator cache. Partitions are grouped
     * by their {@code __mirror_state} coordinator partition and each group is submitted
     * as a read operation through the runtime event queue, guaranteeing that reads are
     * ordered after any preceding writes to the same coordinator partition.
     */
    public void readMirrorStates(String mirrorName,
                                 Map<String, Set<Integer>> partitions,
                                 Consumer<ReadMirrorStatesResponse> callback) {
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
                (shard, offset) -> shard.readMirrorStates(mirrorName, tps)));
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
     * Writes partition states and last mirror epochs received from a remote coordinator.
     * Partitions are grouped by their {@code __mirror_state} coordinator partition and
     * each group is submitted as a single write operation through the runtime event queue,
     * batching state transitions and LME updates into one atomic write per coordinator partition.
     */
    public void writeMirrorStates(String mirrorName,
                                  Map<String, Set<MirrorStateWrite>> mirrorStates,
                                  Consumer<WriteMirrorStatesResponse> callback) {
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

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        byCoordPartition.forEach((cp, states) -> {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, cp);
            futures.add(runtime.scheduleWriteOperation("write-mirror-states", mirrorStateTp,
                Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                shard -> shard.writeMirrorStates(mirrorName, states)));
        });

        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .whenComplete((v, e) -> {
                WriteMirrorStatesResponseData data = new WriteMirrorStatesResponseData();
                if (e != null) {
                    log.error("Failed to write mirror states for {}", mirrorName, e);
                    data.setErrorCode(Errors.forException(e).code());
                    data.setErrorMessage(e.getMessage());
                } else {
                    List<WriteMirrorStatesResponseData.TopicResult> topicResults = new ArrayList<>();
                    tps.forEach((topic, indices) -> {
                        List<WriteMirrorStatesResponseData.PartitionResult> partitionResults = new ArrayList<>();
                        indices.forEach(i -> {
                            WriteMirrorStatesResponseData.PartitionResult pr = new WriteMirrorStatesResponseData.PartitionResult();
                            pr.setPartitionIndex(i);
                            pr.setErrorCode((short) 0);
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

    // ---------------------------------------------------------------
    // Private: update LME, truncation, failed retries, stopping
    // ---------------------------------------------------------------

    public CompletableFuture<Void> updateLastMirrorEpoch(String mirrorName, TopicPartition tp, int epoch) {
        throwIfNotActive();
        if (epoch == -1) {
            return CompletableFuture.completedFuture(null);
        }
        bridge.setLastMirrorEpoch(mirrorName, tp.topic(), tp.partition(), epoch);

        if (isLocal(mirrorName, tp)) {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME,
                    partitionFor(MirrorPartitionKey.of(mirrorName, bridge.getTopicId(tp.topic()), tp.partition())));
            return runtime.scheduleWriteOperation("update-lme", mirrorStateTp,
                    Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                    shard -> shard.updateLastMirrorEpoch(mirrorName, tp, epoch));
        } else {
            bridge.writeStatesToRemoteCoordinator(mirrorName,
                    Map.of(tp.topic(), Set.of(new MirrorStateWrite(tp.partition(), null, epoch))),
                    Set.of(), res -> { });
            return CompletableFuture.completedFuture(null);
        }
    }

    private void scheduleTruncation(String mirrorName, TopicPartition tp) {
        final Consumer<TopicPartition> truncateCallback =
            partition -> transitionTo(mirrorName, Set.of(partition), MirrorPartitionState.MIRRORING);
        scheduler.scheduleOnce("truncation-" + mirrorName + "-" + tp,
            () -> {
                try {
                    var sourceMirrors = bridge.listSourceClusterMirrors(mirrorName);
                    if (bridge.hasMirrorLoop(mirrorName, tp, sourceMirrors)) {
                        transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED,
                            "Detected mirror loop for mirror:" + mirrorName);
                        return;
                    }
                    bridge.sendLastMirrorEpochLookup(mirrorName, tp, sourceMirrors)
                        .whenComplete((epochs, rawError) -> {
                            if (rawError != null) {
                                Throwable error = rawError instanceof CompletionException && rawError.getCause() != null
                                    ? rawError.getCause() : rawError;
                                if (error instanceof UnsupportedVersionException) {
                                    log.warn("Source cluster doesn't support DescribeClusterMirror API. " +
                                        "Replication will be one-way without failback");
                                    bridge.maybeTruncateForLeaderEpoch(Map.of(tp, -1), truncateCallback);
                                } else {
                                    log.warn("Failed to truncate to last mirrored epoch for mirror {}", mirrorName, error);
                                    transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, error.getMessage());
                                }
                                return;
                            }
                            if (!epochs.containsKey(tp)) {
                                log.warn("No epoch returned for {}. Using -1.", tp);
                                epochs.put(tp, -1);
                            }
                            bridge.maybeTruncateForLeaderEpoch(epochs, truncateCallback);
                        });
                } catch (Exception e) {
                    log.warn("Failed to truncate to last mirror epochs for mirror {}", mirrorName, e);
                    transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, e.getMessage());
                }
            }, 0);
    }

    private CompletableFuture<Void> bumpLeaderEpoch(TopicPartition tp) {
        return bridge.bumpLeaderEpochs(bridge.getLatestLocalEpoch(tp));
    }

    private CompletableFuture<Void> abortOngoingTransactions(TopicPartition tp) {
        return bridge.abortOngoingTransactions(tp);
    }

    private CompletableFuture<Void> writePidResetBarrier(String mirrorName, TopicPartition tp) {
        String sourceClusterId = bridge.getSourceClusterId(mirrorName);
        if (sourceClusterId == null) {
            log.warn("Source cluster ID not available for mirror {}. Skipping PID reset barrier.", mirrorName);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        bridge.appendPidResetBarrier(tp, sourceClusterId, time.milliseconds())
            .whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to write PID reset record for {} in mirror {}", tp, mirrorName, ex);
                    scheduler.scheduleOnce("pid-reset-retry-" + tp,
                        () -> writePidResetBarrier(mirrorName, tp).thenAccept(r -> result.complete(null)), 5000);
                } else {
                    result.complete(null);
                }
            });
        return result;
    }

    private void scheduleFailedRetry(String mirrorName, TopicPartition tp) {
        int maxAttempts = config.failedRetryMaxAttempts();
        MirrorPartition mp = MirrorPartition.orEmpty(bridge.getPartition(
                MirrorPartitionKey.of(mirrorName, bridge.getTopicId(tp.topic()), tp.partition())));
        int attempt = mp.retryAttempt() != 0 ? mp.retryAttempt() : 1;
        if (attempt == MirrorPartition.NON_RETRYABLE_ATTEMPT) {
            log.debug("Skipping retry for partition {} because it is in non-retryable failed state.", tp);
            return;
        }
        if (attempt >= maxAttempts) {
            log.error("Partition {} exceeded max retry attempts ({}), requires manual intervention.", tp, maxAttempts);
            return;
        }
        ExponentialBackoff failedRetryBackoff = new ExponentialBackoff(
                config.failedRetryInitialBackoffMs(),
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                config.failedRetryMaxBackoffMs(),
                CommonClientConfigs.RETRY_BACKOFF_JITTER);
        long delay = failedRetryBackoff.backoff(attempt);
        MirrorPartitionState targetState;
        if (mp.prevState() == null || mp.prevState() == MirrorPartitionState.UNKNOWN) {
            targetState = MirrorPartitionState.LOG_TRUNCATION;
        } else {
            targetState = mp.prevState();
        }
        log.info("Scheduling retry attempt #{} for partition {} in {} ms with target state {}.",
                attempt, tp, delay, targetState);
        scheduler.scheduleOnce("failed-retry-" + tp,
                () -> transitionTo(mirrorName, Set.of(tp), targetState), delay);
    }

    // ---------------------------------------------------------------
    // Private: tombstones
    // ---------------------------------------------------------------

    private void tombstoneMirror(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> states = bridge.getMirrorStates(mirrorName);
        Map<Integer, Set<TopicPartition>> coordPartitionToMirrorPartitions = new HashMap<>();
        states.forEach((tp, state) -> {
            if (isLocal(mirrorName, tp)) {
                coordPartitionToMirrorPartitions.computeIfAbsent(
                        partitionFor(MirrorPartitionKey.of(mirrorName, bridge.getTopicId(tp.topic()), tp.partition())),
                        v -> new HashSet<>()).add(tp);
            }
        });

        bridge.removeMirror(mirrorName);
        bridge.removePendingEpochBumps(states.keySet());

        if (coordPartitionToMirrorPartitions.isEmpty()) {
            states.keySet().forEach(tp ->
                    bridge.removePartition(
                            MirrorPartitionKey.of(mirrorName, bridge.getTopicId(tp.topic()), tp.partition())));
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
                            tps.forEach(tp -> bridge.removePartition(
                                    MirrorPartitionKey.of(mirrorName,
                                            bridge.getTopicId(tp.topic()), tp.partition())));
                        }
                    });
        }
    }

    public record MirrorStateWrite(int partition, MirrorPartitionState state, Integer leaderEpoch) { }
}
