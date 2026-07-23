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
import kafka.server.mirror.MirrorMetadataManager.FailedPartitionInfo;
import kafka.server.mirror.MirrorMetadataManager.PartitionStateInfo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DeleteClusterMirrorRequestData;
import org.apache.kafka.common.message.MirrorPidResetRecord;
import org.apache.kafka.common.message.PauseMirrorTopicsRequestData;
import org.apache.kafka.common.message.ResumeMirrorTopicsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;
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
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinator;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorShard;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorShard.MetadataManagerBridge;
import org.apache.kafka.coordinator.mirror.ClusterMirrorPartitionKey;
import org.apache.kafka.coordinator.mirror.ClusterMirrorRecordSerde;
import org.apache.kafka.coordinator.mirror.metrics.ClusterMirrorCoordinatorMetrics;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.config.ClusterMirrorConfig;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
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


import scala.jdk.javaapi.CollectionConverters;

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
    private final MirrorMetadataManager metadataManager;
    private final MetadataCache metadataCache;
    private final ReplicaManager replicaManager;
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
        private MirrorMetadataManager metadataManager;
        private MetadataCache metadataCache;
        private ReplicaManager replicaManager;
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

        public Builder withMetadataManager(MirrorMetadataManager metadataManager) {
            this.metadataManager = metadataManager;
            return this;
        }

        public Builder withMetadataCache(MetadataCache metadataCache) {
            this.metadataCache = metadataCache;
            return this;
        }

        public Builder withReplicaManager(ReplicaManager replicaManager) {
            this.replicaManager = replicaManager;
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

            MetadataManagerBridgeImpl metadataManagerBridge = new MetadataManagerBridgeImpl(metadataManager, metadataCache);

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
                            () -> new ClusterMirrorCoordinatorShard.Builder(metadataManagerBridge, numPartitions))
                    .withDefaultWriteTimeOut(Duration.ofMillis(config.coordinatorWriteTimeoutMs()))
                    .withCoordinatorRuntimeMetrics(runtimeMetrics)
                    .withCoordinatorMetrics(new ClusterMirrorCoordinatorMetrics())
                    .withSerializer(new ClusterMirrorRecordSerde())
                    .withAppendLingerMs(config.coordinatorAppendLingerMs())
                    .withExecutorService(Executors.newSingleThreadExecutor())
                    .build();

            return new ClusterMirrorCoordinatorService(
                    nodeId, config, runtime, metadataManager, metadataCache,
                    replicaManager, scheduler, metrics, time);
        }
    }

    ClusterMirrorCoordinatorService(
        int nodeId,
        ClusterMirrorConfig config,
        CoordinatorRuntime<ClusterMirrorCoordinatorShard, CoordinatorRecord> runtime,
        MirrorMetadataManager metadataManager,
        MetadataCache metadataCache,
        ReplicaManager replicaManager,
        Scheduler scheduler,
        Metrics metrics,
        Time time
    ) {
        String name = "[ClusterMirrorCoordinatorService id=" + nodeId + "] ";
        this.log = new LogContext(name).logger(ClusterMirrorCoordinatorService.class);
        this.nodeId = nodeId;
        this.config = config;
        this.runtime = runtime;
        this.metadataManager = metadataManager;
        this.metadataCache = metadataCache;
        this.replicaManager = replicaManager;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.time = time;
    }

    // ---------------------------------------------------------------
    // Lifecycle (ClusterMirrorCoordinator interface)
    // ---------------------------------------------------------------

    @Override
    public void start() {
        if (!isActive.compareAndSet(false, true)) {
            log.warn("Is already running.");
            return;
        }
        log.info("Starting up.");
        metadataManager.initialize(
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
        metadataManager.closeSourceAdmins();
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
    // Side effects (after successful write commit)
    // ---------------------------------------------------------------

    private void handleSideEffect(String mirrorName, TopicPartition tp, MirrorPartitionState newState) {
        switch (newState) {
            case LOG_TRUNCATION:
                log.info("Mirror '{}' transitioning {} to LOG_TRUNCATION.", mirrorName, tp);
                scheduleTruncation(mirrorName, tp);
                break;
            case EPOCH_FENCING:
                log.info("Mirror '{}' transitioning {} to EPOCH_FENCING.", mirrorName, tp);
                metadataManager.scheduleBumpLeaderEpoch(mirrorName, tp)
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
                replicaManager.maybeCreateMirrorFetchers(mirrorName, Set.of(tp));
                break;
            case PAUSING:
                log.info("Mirror '{}' transitioning {} to PAUSING.", mirrorName, tp);
                replicaManager.mirrorFetcherManager().removeFetcherForPartitions(
                        CollectionConverters.asScala(Set.of(tp)));
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED);
                break;
            case PAUSED:
                log.info("Mirror '{}' transitioning {} to PAUSED.", mirrorName, tp);
                break;
            case STOPPING:
                log.info("Mirror '{}' transitioning {} to STOPPING.", mirrorName, tp);
                // 1. Remove fetcher for mirror fetcher
                // 2. Store LME because that's the last mirrored epoch before bumping leader epoch
                // 3. Bump leader epoch to draw a line for future failback cluster recognize the new added records
                // 4. Abort ongoing transactions using the updated leader epoch
                // 5. Reset pid to expire all existing PIDs, including the new appended records in (4)
                // 6. Move to STOPPED state
                replicaManager.mirrorFetcherManager().removeFetcherForPartitions(
                        CollectionConverters.asScala(Set.of(tp)));
                var logOpt = replicaManager.getPartitionOrException(tp).log();
                int latestEpoch = logOpt.isDefined() ? logOpt.get().latestEpoch().orElse(-1) : -1;
                updateLastMirrorEpoch(mirrorName, tp, latestEpoch)
                        .thenCompose(v -> bumpLeaderEpoch(tp))
                        .thenCompose(v -> abortOngoingTransactions(tp))
                        .thenCompose(v -> writePidResetBarrier(mirrorName, tp))
                        .thenAccept(v -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPED))
                        .exceptionally(ex -> {
                            log.error("Mirror '{}' STOPPING transition failed for {}.", mirrorName, tp, ex);
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
                mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition()));
        var opt = metadataCache.getLeaderAndIsr(MIRROR_STATE_TOPIC_NAME, partition);
        return opt.isPresent() && opt.get().leader() == nodeId;
    }

    // ---------------------------------------------------------------
    // State transitions
    // ---------------------------------------------------------------

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
        topicPartitions.forEach(tp -> {
            if (isLocal(mirrorName, tp)) {
                TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME,
                    partitionFor(ClusterMirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())));
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
                        handleSideEffect(mirrorName, tp, newState);
                    });
            } else {
                Map<String, Set<PartitionStateInfo>> topicMetadata =
                    Map.of(tp.topic(), Set.of(new PartitionStateInfo(tp.partition(), newState, -1)));
                metadataManager.writeStatesToRemoteCoordinator(mirrorName, topicMetadata, Set.of(),
                    res -> res.data().topics().forEach(topic -> topic.partitions().forEach(par -> {
                        if (par.errorCode() == Errors.NONE.code()) {
                            ClusterMirrorPartitionKey key = ClusterMirrorPartitionKey.of(mirrorName,
                                metadataCache.getTopicId(tp.topic()), tp.partition());
                            updateLocalFailedState(key, newState, errorMessage, nonRetryable);
                            metadataManager.setPartitionState(key, newState);
                            handleSideEffect(mirrorName, tp, newState);
                        } else {
                            log.error("Failed to write partition state to remote coordinator: {}", par.errorCode());
                        }
                    })));
            }
        });
    }

    private void updateLocalFailedState(ClusterMirrorPartitionKey key,
                                        MirrorPartitionState newState,
                                        String errorMessage, boolean nonRetryable) {
        MirrorPartitionState curState = metadataManager.getPartitionState(
                key.mirrorName(), new TopicPartition(
                        metadataCache.getTopicName(key.topicId()).orElse(""), key.partition()));
        metadataManager.updateFailedState(key, curState, newState, errorMessage, nonRetryable);
    }

    // ---------------------------------------------------------------
    // LME operations
    // ---------------------------------------------------------------

    public CompletableFuture<Void> updateLastMirrorEpoch(String mirrorName, TopicPartition tp, int epoch) {
        if (epoch == -1) {
            return CompletableFuture.completedFuture(null);
        }
        metadataManager.setLastMirrorEpoch(mirrorName, tp.topic(), tp.partition(), epoch);

        if (isLocal(mirrorName, tp)) {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME,
                partitionFor(ClusterMirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())));
            return runtime.scheduleWriteOperation("update-lme", mirrorStateTp,
                Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                shard -> shard.updateLastMirrorEpoch(mirrorName, tp, epoch));
        } else {
            metadataManager.writeStatesToRemoteCoordinator(mirrorName,
                Map.of(tp.topic(), Set.of(new PartitionStateInfo(tp.partition(), null, epoch))),
                Set.of(), res -> { });
            return CompletableFuture.completedFuture(null);
        }
    }

    // ---------------------------------------------------------------
    // Inter-broker RPC handling
    // ---------------------------------------------------------------

    public void writeMirrorStates(String mirrorName,
                                  Map<String, Set<PartitionStateInfo>> mirrorStates,
                                  Consumer<WriteMirrorStatesResponse> callback) {
        List<CompletableFuture<?>> stateFutures = new ArrayList<>();
        List<CompletableFuture<Void>> lmeFutures = new ArrayList<>();
        Map<String, Set<Integer>> tps = new HashMap<>();

        mirrorStates.forEach((topic, partitions) -> {
            Set<Integer> partitionIndices = new HashSet<>();
            partitions.forEach(partition -> {
                TopicPartition tp = new TopicPartition(topic, partition.partition());
                partitionIndices.add(tp.partition());
                if (partition.state() != null && partition.state() != MirrorPartitionState.UNKNOWN) {
                    TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME,
                        partitionFor(ClusterMirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())));
                    stateFutures.add(runtime.scheduleWriteOperation(
                        "write-state", mirrorStateTp,
                        Duration.ofMillis(config.coordinatorWriteTimeoutMs()),
                        shard -> shard.transitionTo(mirrorName, tp, partition.state(), null, false)));
                }
                if (partition.leaderEpoch() != -1) {
                    TopicPartition lmeTp = new TopicPartition(topic, partition.partition());
                    lmeFutures.add(updateLastMirrorEpoch(mirrorName, lmeTp, partition.leaderEpoch()));
                }
            });
            tps.put(topic, partitionIndices);
        });

        CompletableFuture<Void> lmeFuture = CompletableFuture.allOf(
            lmeFutures.toArray(CompletableFuture[]::new));
        CompletableFuture.allOf(stateFutures.toArray(CompletableFuture[]::new))
            .thenCompose(v -> lmeFuture)
            .whenComplete((v, e) -> {
                WriteMirrorStatesResponseData data = new WriteMirrorStatesResponseData();
                if (e != null) {
                    log.error("Failed to update partition state and LME for {}: {}", mirrorName, e);
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

    public void readMirrorStates(String mirrorName,
                                 Map<String, Set<Integer>> partitions,
                                 Consumer<ReadMirrorStatesResponse> callback) {
        metadataManager.readMirrorStates(mirrorName, partitions, callback);
    }

    // ---------------------------------------------------------------
    // Private: truncation, failed retries, stopping
    // ---------------------------------------------------------------

    private void scheduleTruncation(String mirrorName, TopicPartition tp) {
        final Consumer<TopicPartition> truncateCallback =
            partition -> transitionTo(mirrorName, Set.of(partition), MirrorPartitionState.MIRRORING);
        scheduler.scheduleOnce("truncation-" + mirrorName + "-" + tp,
            () -> {
                try {
                    var sourceMirrors = metadataManager.listSourceClusterMirrors(mirrorName);
                    if (metadataManager.hasMirrorLoop(mirrorName, tp, sourceMirrors)) {
                        transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED,
                            "Detected mirror loop for mirror:" + mirrorName);
                        return;
                    }
                    metadataManager.sendLastMirrorEpochLookup(mirrorName, tp, sourceMirrors)
                        .whenComplete((epochs, rawError) -> {
                            if (rawError != null) {
                                Throwable error = rawError instanceof CompletionException && rawError.getCause() != null
                                    ? rawError.getCause() : rawError;
                                if (error instanceof UnsupportedVersionException) {
                                    log.warn("Source cluster doesn't support DescribeClusterMirror API. " +
                                        "Replication will be one-way without failback");
                                    replicaManager.maybeTruncateForLeaderEpoch(Map.of(tp, -1), truncateCallback);
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
                            replicaManager.maybeTruncateForLeaderEpoch(epochs, truncateCallback);
                        });
                } catch (Exception e) {
                    log.warn("Failed to truncate to last mirror epochs for mirror {}", mirrorName, e);
                    transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, e.getMessage());
                }
            }, 0);
    }

    private CompletableFuture<Void> abortOngoingTransactions(TopicPartition tp) {
        var record = replicaManager.getLog(tp).map(UnifiedLog::buildEndTransactionRecords);
        if (!record.isDefined() || record.get().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (MemoryRecords memRecords : record.get()) {
            CompletableFuture<Void> batchFuture = new CompletableFuture<>();
            replicaManager.appendRecords(
                    Duration.ofSeconds(5).toMillis(),
                    (short) -1,
                    true,
                    AppendOrigin.COORDINATOR,
                    CollectionConverters.asScala(Map.of(replicaManager.topicIdPartition(tp), memRecords)),
                    partitionResponses -> {
                        batchFuture.complete(null);
                        return null;
                    },
                    ignored -> null,
                    RequestLocal.noCaching(),
                    CollectionConverters.asScala(Map.of()));
            futures.add(batchFuture);
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> bumpLeaderEpoch(TopicPartition tp) {
        return metadataManager.bumpLeaderEpochs(
                metadataManager.getLatestLocalEpoch(replicaManager.logManager(), tp));
    }

    private CompletableFuture<Void> writePidResetBarrier(String mirrorName, TopicPartition tp) {
        String sourceClusterId = metadataManager.getSourceClusterId(mirrorName);
        if (sourceClusterId == null) {
            log.warn("Source cluster ID not available for mirror {}. Skipping PID reset barrier.", mirrorName);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture<ProduceResponse.PartitionResponse> future = new CompletableFuture<>();
        MirrorPidResetRecord record = new MirrorPidResetRecord()
            .setVersion(ControlRecordUtils.MIRROR_PID_RESET_CURRENT_VERSION)
            .setSourceClusterId(sourceClusterId);
        try {
            var topicIdPartition = replicaManager.topicIdPartition(tp);
            int bufferSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD + 256;
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            MemoryRecords records = MemoryRecords.withMirrorPidResetRecord(
                0, time.milliseconds(), 0, buffer, record);
            replicaManager.appendRecords(
                Duration.ofSeconds(5).toMillis(),
                (short) -1,
                true,
                AppendOrigin.COORDINATOR,
                CollectionConverters.asScala(Map.of(topicIdPartition, records)),
                partitionResponses -> {
                    partitionResponses.foreach(partitionRes -> {
                        future.complete(partitionRes._2);
                        return null;
                    });
                    return null;
                },
                ignored -> null,
                RequestLocal.noCaching(),
                CollectionConverters.asScala(Map.of()));
        } catch (Exception e) {
            log.error("Failed to write PID reset barrier for {} in mirror {}", tp, mirrorName, e);
            future.completeExceptionally(e);
        }
        future.whenComplete((pr, ex) -> {
            if (ex != null) {
                log.error("Failed to write PID reset record for {} in mirror {}", tp, mirrorName, ex);
                scheduler.scheduleOnce("pid-reset-retry-" + tp,
                    () -> writePidResetBarrier(mirrorName, tp).thenAccept(v -> result.complete(null)), 5000);
            } else if (pr == null || pr.error.code() != Errors.NONE.code()) {
                log.warn("PID reset barrier error for {} in mirror {}: {}",
                    tp, mirrorName, pr != null ? pr.error.message() : "no response");
                scheduler.scheduleOnce("pid-reset-retry-" + tp,
                    () -> writePidResetBarrier(mirrorName, tp).thenAccept(v -> result.complete(null)), 5000);
            } else {
                result.complete(null);
            }
        });
        return result;
    }

    private void scheduleFailedRetry(String mirrorName, TopicPartition tp) {
        int maxAttempts = config.failedRetryMaxAttempts();
        FailedPartitionInfo fpi = metadataManager.getFailedInfo(
                ClusterMirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition()));
        int attempt = fpi != null ? fpi.retryAttempt() : 1;
        if (attempt == MirrorMetadataManager.NON_RETRYABLE_ATTEMPT) {
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
        if (fpi == null) {
            targetState = MirrorPartitionState.MIRRORING;
        } else if (fpi.previousState() == MirrorPartitionState.UNKNOWN) {
            targetState = MirrorPartitionState.LOG_TRUNCATION;
        } else {
            targetState = fpi.previousState();
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
        Map<TopicPartition, MirrorPartitionState> states = metadataManager.getMirrorStates(mirrorName);
        Map<Integer, Set<TopicPartition>> coordPartitionToMirrorPartitions = new HashMap<>();
        states.forEach((tp, state) -> {
            if (isLocal(mirrorName, tp)) {
                coordPartitionToMirrorPartitions.computeIfAbsent(
                        partitionFor(ClusterMirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())),
                        v -> new HashSet<>()).add(tp);
            }
        });

        metadataManager.removeCachedMirror(mirrorName);
        metadataManager.removeStateForPartitions(states.keySet());

        if (coordPartitionToMirrorPartitions.isEmpty()) {
            states.keySet().forEach(tp ->
                    metadataManager.clearPartitionState(
                            ClusterMirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())));
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
                            tps.forEach(tp -> metadataManager.clearPartitionState(
                                    ClusterMirrorPartitionKey.of(mirrorName,
                                            metadataCache.getTopicId(tp.topic()), tp.partition())));
                        }
                    });
        }
    }

    // ---------------------------------------------------------------
    // Delegation to MirrorMetadataManager
    // ---------------------------------------------------------------

    public String getSourceClusterId(String mirrorName) {
        return metadataManager.getSourceClusterId(mirrorName);
    }

    public String getSourceBootstrap(String mirrorName) {
        return metadataManager.getSourceBootstrap(mirrorName);
    }

    public Set<String> getConfiguredMirrors() {
        return metadataManager.getConfiguredMirrors();
    }

    public Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        return metadataManager.getMirrorStates(mirrorName);
    }

    public void validateStartMirrorStates(StartMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateStartMirrorStates(data, callback);
    }

    public void validateStopMirrorStates(StopMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateStopMirrorStates(data, callback);
    }

    public void validatePauseMirrorStates(PauseMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validatePauseMirrorStates(data, callback);
    }

    public void validateResumeMirrorStates(ResumeMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateResumeMirrorStates(data, callback);
    }

    public void validateDeleteMirrorStates(DeleteClusterMirrorRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateDeleteMirrorStates(data, callback);
    }

    public Set<String> getConfiguredTopics(String mirrorName, boolean includePaused, boolean includeStopped) {
        return metadataManager.getConfiguredTopics(mirrorName, includePaused, includeStopped);
    }

    public int getActiveTopicCount(String mirrorName) {
        return metadataManager.getActiveTopicCount(mirrorName);
    }

    public FailedPartitionInfo getFailedInfo(String mirrorName, TopicPartition tp) {
        return metadataManager.getFailedInfo(
                ClusterMirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition()));
    }

    public Map<String, Map<TopicPartition, Integer>> processLastMirrorEpochLookup(
            Map<String, Map<String, Set<Integer>>> mirrorPartitions) {
        return metadataManager.processLastMirrorEpochLookup(mirrorPartitions);
    }

    public void scheduleMetadataRefresh(long intervalMs) {
        metadataManager.scheduleMetadataRefresh(intervalMs);
    }

    static class MetadataManagerBridgeImpl implements MetadataManagerBridge {
        private final MirrorMetadataManager metadataManager;
        private final MetadataCache metadataCache;

        MetadataManagerBridgeImpl(MirrorMetadataManager metadataManager, MetadataCache metadataCache) {
            this.metadataManager = metadataManager;
            this.metadataCache = metadataCache;
        }

        @Override
        public void onShardLoaded() {
            metadataManager.processAllStateTransitions();
        }

        @Override
        public void onShardUnloaded(int partitionIndex, int numPartitions) {
            metadataManager.clearCacheForPartition(partitionIndex, numPartitions);
        }

        @Override
        public void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch) {
            metadataManager.setLastMirrorEpoch(mirrorName, topic, partition, epoch);
        }

        @Override
        public MirrorPartitionState getPartitionState(String mirrorName, TopicPartition topicPartition) {
            return metadataManager.getPartitionState(mirrorName, topicPartition);
        }

        @Override
        public void setPartitionState(ClusterMirrorPartitionKey key, MirrorPartitionState newState) {
            metadataManager.setPartitionState(key, newState);
        }

        @Override
        public void clearPartitionState(ClusterMirrorPartitionKey key) {
            metadataManager.clearPartitionState(key);
        }

        @Override
        public ClusterMirrorCoordinatorShard.FailedPartitionInfo getFailedInfo(ClusterMirrorPartitionKey key) {
            FailedPartitionInfo fpi = metadataManager.getFailedInfo(key);
            if (fpi == null) return null;
            return new ClusterMirrorCoordinatorShard.FailedPartitionInfo(
                fpi.retryAttempt(), fpi.errorMessage(), fpi.previousState());
        }

        @Override
        public void setFailedInfo(ClusterMirrorPartitionKey key,
                                  ClusterMirrorCoordinatorShard.FailedPartitionInfo info) {
            metadataManager.setFailedInfo(key,
                new FailedPartitionInfo(info.retryAttempt(), info.errorMessage(), info.previousState()));
        }

        @Override
        public void clearFailedInfo(ClusterMirrorPartitionKey key) {
            metadataManager.clearFailedInfo(key);
        }

        @Override
        public void updateFailedState(ClusterMirrorPartitionKey key, MirrorPartitionState currentState,
                                      MirrorPartitionState newState, String errorMessage, boolean nonRetryable) {
            metadataManager.updateFailedState(key, currentState, newState, errorMessage, nonRetryable);
        }

        @Override
        public Uuid getTopicId(String topicName) {
            return metadataCache.getTopicId(topicName);
        }

        @Override
        public Optional<String> getTopicName(Uuid topicId) {
            return metadataCache.getTopicName(topicId);
        }

    }
}
