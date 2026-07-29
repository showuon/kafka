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

import kafka.server.KafkaConfig;
import kafka.server.NetworkUtils;
import kafka.server.ReplicaManager;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DeleteClusterMirrorRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MirrorPidResetRecord;
import org.apache.kafka.common.message.PauseMirrorTopicsRequestData;
import org.apache.kafka.common.message.ReadMirrorStatesRequestData;
import org.apache.kafka.common.message.ResumeMirrorTopicsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesRequest;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorService.MirrorStateWrite;
import org.apache.kafka.coordinator.mirror.CoreBridge;
import org.apache.kafka.coordinator.mirror.MirrorPartition;
import org.apache.kafka.coordinator.mirror.MirrorPartitionKey;
import org.apache.kafka.image.LocalReplicaChanges;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Reacts to KRaft metadata changes, decides mirror partition state transitions,
 * persists them via {@link CoreBridge.CoordinatorWriter}, and executes the
 * resulting side effects (truncation, fetcher lifecycle, epoch bumps, retries).
 * Delegates periodic source cluster synchronization to {@link MirrorSourceSyncer}.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class MirrorMetadataManager implements MetadataPublisher, AutoCloseable {
    private static final Set<String> NON_CONNECTION_CONFIGS = Set.of(
            ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, ClusterMirrorConfig.MIRROR_TOPICS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.MIRROR_GROUPS_INCLUDE_CONFIG, ClusterMirrorConfig.MIRROR_GROUPS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.MIRROR_ACL_INCLUDE_CONFIG);

    private final Logger log;
    private volatile boolean isInitialized = false;
    private final String clusterId;
    private final KafkaConfig brokerConfig;
    private final String name;
    private final int nodeId;

    private final NodeToControllerChannelManager channelManager;
    private final Supplier<ReplicaManager> replicaManagerSupplier;
    private volatile MetadataImage metadataImage = MetadataImage.EMPTY;
    private final MetadataCache metadataCache;
    private final MirrorStateCache mirrorCache;
    private final KafkaScheduler scheduler;
    private final Metrics metrics;
    private final Time time;

    private volatile MirrorSourceSyncer sourceSyncer;
    private volatile MirrorStateSender mirrorStateSender;
    private volatile Map<String, Admin> srcAdmins;
    private volatile Admin dstAdmin;

    private Optional<CoreBridge.CoordinatorWriter> coordinatorWriter = Optional.empty();
    private Optional<Function<MirrorPartitionKey, Integer>> coordPartFinder = Optional.empty();

    private final AtomicLong metadataRefreshError;
    private final AtomicLong topicConfigSyncError;
    private final AtomicLong consumerGroupOffsetSyncError;
    private final AtomicLong shareGroupOffsetSyncError;
    private final AtomicLong aclSyncError;

    public MirrorMetadataManager(
        String clusterId,
        KafkaConfig brokerConfig,
        NodeToControllerChannelManager channelManager,
        Supplier<ReplicaManager> replicaManagerSupplier,
        MetadataCache metadataCache,
        KafkaScheduler scheduler,
        Metrics metrics,
        Time time
    ) {
        this.clusterId = clusterId;
        this.brokerConfig = brokerConfig;
        this.nodeId = brokerConfig.nodeId();
        this.name = "[" + MirrorMetadataManager.class.getSimpleName() + " id=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(MirrorMetadataManager.class);

        this.channelManager = channelManager;
        this.replicaManagerSupplier = replicaManagerSupplier;
        this.metadataCache = metadataCache;
        this.mirrorCache = MirrorStateCache.empty();

        this.scheduler = scheduler;
        this.metrics = metrics;
        this.time = time;

        KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());
        this.metadataRefreshError = new AtomicLong();
        this.topicConfigSyncError = new AtomicLong();
        this.consumerGroupOffsetSyncError = new AtomicLong();
        this.shareGroupOffsetSyncError = new AtomicLong();
        this.aclSyncError = new AtomicLong();

        metricsGroup.newGauge("TopicConfigSyncError", topicConfigSyncError::get);
        metricsGroup.newGauge("ConsumerGroupOffsetSyncError", consumerGroupOffsetSyncError::get);
        metricsGroup.newGauge("ShareGroupOffsetSyncError", shareGroupOffsetSyncError::get);
        metricsGroup.newGauge("AclSyncError", aclSyncError::get);
        metricsGroup.newGauge("TopicMetadataRefreshError", metadataRefreshError::get);
        metricsGroup.newGauge("LogTruncationPartitionState", () -> mirrorCache.partitionStateCount(MirrorPartitionState.LOG_TRUNCATION));
        metricsGroup.newGauge("EpochFencingPartitionState", () -> mirrorCache.partitionStateCount(MirrorPartitionState.EPOCH_FENCING));
        metricsGroup.newGauge("MirroringPartitionState", () -> mirrorCache.partitionStateCount(MirrorPartitionState.MIRRORING));
        metricsGroup.newGauge("PausingPartitionState", () -> mirrorCache.partitionStateCount(MirrorPartitionState.PAUSING));
        metricsGroup.newGauge("PausedPartitionState", () -> mirrorCache.partitionStateCount(MirrorPartitionState.PAUSED));
        metricsGroup.newGauge("StoppingPartitionState", () -> mirrorCache.partitionStateCount(MirrorPartitionState.STOPPING));
        metricsGroup.newGauge("StoppedPartitionState", () -> mirrorCache.partitionStateCount(MirrorPartitionState.STOPPED));
        metricsGroup.newGauge("FailedPartitionState", () -> mirrorCache.partitionStateCount(MirrorPartitionState.FAILED));
    }

    /**
     * Checks whether this broker leads the __mirror_state partition for the given mirror partition.
     * Hashes by composite key (mirror name, topic id, partition), distributing partition-level
     * coordination across brokers so a mirror with many partitions does not bottleneck on one node.
     */
    private boolean isLocalCoordinator(String mirrorName, String topic, int partition) {
        if (metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME) != null && coordPartFinder.isPresent()) {
            int activeCoordinator = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordPartFinder.get().apply(
                            MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(topic), partition))).leader;
            return activeCoordinator == brokerConfig.nodeId();
        }
        return false;
    }

    /**
     * Called by ClusterMirrorCoordinator on startup.
     * Creates and starts the MirrorStateSender used for WriteMirrorStates and ReadMirrorStates RPCs.
     * Wires in the state transitioner, tombstone handler, and coordinator partition finders.
     * Creates the {@link MirrorSourceSyncer} and schedules periodic metadata refresh.
     */
    public void initialize(CoreBridge.CoordinatorWriter coordinatorWriter,
                           Function<MirrorPartitionKey, Integer> coordPartFinder) {
        if (mirrorStateSender == null) {
            this.mirrorStateSender = new MirrorStateSender(MirrorStateSender.class.getSimpleName(),
                    NetworkUtils.buildNetworkClient(MirrorMetadataManager.class.getSimpleName(), brokerConfig, metrics, time, new LogContext(name())),
                    brokerConfig.requestTimeoutMs(), Time.SYSTEM);
            mirrorStateSender.start();
        }

        this.sourceSyncer = new MirrorSourceSyncer(brokerConfig, this,
                channelManager, metadataCache, mirrorCache, scheduler, metadataRefreshError,
                topicConfigSyncError, consumerGroupOffsetSyncError, shareGroupOffsetSyncError, aclSyncError);
        sourceSyncer.scheduleMetadataRefresh(brokerConfig.mirrorConfig().metadataRefreshIntervalMs());

        // MMM call the writer whenever it needs to persist state to the __mirror_state shard
        this.coordinatorWriter = Optional.of(coordinatorWriter);

        this.coordPartFinder = Optional.of(coordPartFinder);
        this.isInitialized = true;
    }

    Admin getOrCreateSourceAdmin(String mirrorName) {
        if (srcAdmins == null) {
            srcAdmins = new ConcurrentHashMap<>();
        }
        return srcAdmins.computeIfAbsent(mirrorName, k -> {
            Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, k));
            props.put(AdminClientConfig.CLIENT_ID_CONFIG, "mirror-src-admin-" + k + "-" + nodeId);
            return Admin.create(props);
        });
    }

    Admin getOrCreateDestAdmin() {
        if (dstAdmin == null) {
            Properties props = buildDestAdminClientProps(brokerConfig);
            // Fall back to metadataCache when the advertised port is unresolved (e.g. ephemeral port 0 in tests)
            if (props.getProperty(BOOTSTRAP_SERVERS_CONFIG).endsWith(":0")) {
                ListenerName listenerName = brokerConfig.mirrorAdminListenerName();
                metadataCache.getAliveBrokerNode(nodeId, listenerName).ifPresent(node ->
                        props.put(BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port()));
            }
            dstAdmin = Admin.create(props);
        }
        return dstAdmin;
    }

    // Visible for testing
    static Properties buildDestAdminClientProps(KafkaConfig brokerConfig) {
        ListenerName mirrorAdminListener = brokerConfig.mirrorAdminListenerName();
        Endpoint endpoint = (Endpoint) brokerConfig.effectiveAdvertisedBrokerListeners()
                .filter(e -> e.listener().equals(mirrorAdminListener.value()))
                .head();

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, endpoint.host() + ":" + endpoint.port());
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "mirror-dst-admin-" + brokerConfig.nodeId());

        SecurityProtocol securityProtocol = endpoint.securityProtocol();
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);

        Map<String, ?> configs = ChannelBuilders.channelBuilderConfigs(brokerConfig, mirrorAdminListener);

        // Get all the security configs
        ConfigDef securityConfigDef = new ConfigDef().withClientSaslSupport().withClientSslSupport();
        Set<String> securityConfigs = new HashSet<>(securityConfigDef.configKeys().keySet());

        String mirrorAdminSaslMechanism = brokerConfig.saslMechanismMirrorAdminProtocol();
        if (securityProtocol == SecurityProtocol.SASL_SSL || securityProtocol == SecurityProtocol.SASL_PLAINTEXT) {
            props.put(SaslConfigs.SASL_MECHANISM, mirrorAdminSaslMechanism);
        }

        String saslMechanismConfigPrefix = mirrorAdminListener.saslMechanismConfigPrefix(mirrorAdminSaslMechanism);
        Map<String, ?> saslMechanismConfigs = brokerConfig.originalsWithPrefix(saslMechanismConfigPrefix, true);

        securityConfigs.forEach(key -> {
            if (key.equals(SaslConfigs.SASL_MECHANISM)) return;
            if (saslMechanismConfigs.containsKey(key)) {
                props.put(key, saslMechanismConfigs.get(key));
            } else if (configs.containsKey(key)) {
                Object value = configs.get(key);
                if (value == null) {
                    return;
                }
                props.put(key, value);
            }
        });

        return props;
    }

    @Override
    public String name() {
        return name;
    }

    MetadataImage metadataImage() {
        return metadataImage;
    }

    String clusterId() {
        return clusterId;
    }

    Supplier<ReplicaManager> replicaManagerSupplier() {
        return replicaManagerSupplier;
    }

    /**
     * Called when cluster metadata is updated and it is executed in the KRaft metadata publisher thread.
     * Detects mirror partition leadership changes and triggers state transitions via batched coordinator reads.
     * On connection config changes, source connections are recreated, source metadata is fetched eagerly,
     * and fetchers are re-created for affected MIRRORING partitions.
     * <p>
     * This method must be called after ReplicaManager#applyDelta.
     * The metadataCache can't be used here because it is updated concurrently.
     */
    public void updateMetadataImage(MetadataImage newImage) {
        this.metadataImage = newImage;
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (!isInitialized) {
            return;
        }

        this.metadataImage = newImage;

        Set<String> mirrorsToReconnect = handleMirrorConfigDeltas(delta, newImage);
        Set<TopicPartition> partitionsToTransition =
                collectPartitionsForStateTransition(delta, newImage, mirrorsToReconnect);

        if (partitionsToTransition.isEmpty()) {
            return;
        }

        log.info("Processing metadata update for {} mirror leader partition(s): {}",
                partitionsToTransition.size(), partitionsToTransition);

        processStateTransitions(partitionsToTransition, newImage);
        maybeCompletePendingEpochBumps();
    }

    /**
     * Tears down source connections for mirrors whose config changed or were deleted.
     * Deleted mirrors also get tombstone records written.
     *
     * @return mirrors that need reconnection (excludes deleted ones)
     */
    private Set<String> handleMirrorConfigDeltas(MetadataDelta delta, MetadataImage newImage) {
        Set<String> mirrorsToReconnect = new HashSet<>();
        if (delta.configsDelta() != null) {
            delta.configsDelta().changes().entrySet().stream()
                    .filter(e -> e.getKey().type() == ConfigResource.Type.CLUSTER_MIRROR)
                    .forEach(e -> {
                        String mirrorName = e.getKey().name();
                        boolean mirrorDeleted = newImage.configs().configProperties(e.getKey()).isEmpty();
                        if (mirrorDeleted) {
                            log.info("Mirror '{}' has been deleted. Writing tombstone records.", mirrorName);
                            tombstoneMirror(mirrorName);
                        }

                        boolean connectionConfigChanged = e.getValue().changes().keySet().stream()
                                .anyMatch(key -> !NON_CONNECTION_CONFIGS.contains(key));
                        if (connectionConfigChanged) {
                            log.info("Mirror '{}' has connection config changed. Recreating connections.", mirrorName);
                        }
                        if (connectionConfigChanged || mirrorDeleted) {
                            mirrorCache.removeSourceLeaders(mirrorName);
                            closeAndRemoveSourceAdmin(mirrorName);
                            var mirrorFetcherManager = replicaManagerSupplier.get().mirrorFetcherManager();
                            mirrorFetcherManager.removeFetchersForMirror(mirrorName);
                            mirrorFetcherManager.shutdownIdleFetcherThreads();
                            if (!mirrorDeleted) {
                                mirrorsToReconnect.add(mirrorName);
                            }
                        }
                    });
        }
        return mirrorsToReconnect;
    }

    /**
     * Returns mirror partitions that need a state transition: gained leaders,
     * mirror state changes, and MIRRORING partitions of reconnected mirrors.
     * Also clears cached state for partitions where this broker lost leadership.
     */
    private Set<TopicPartition> collectPartitionsForStateTransition(MetadataDelta delta, MetadataImage image,
                                                                    Set<String> reconnectedMirrors) {
        Set<TopicPartition> partitionsToTransition = new HashSet<>();
        Set<String> configuredMirrors = getConfiguredMirrors();

        if (delta.topicsDelta() != null) {
            LocalReplicaChanges localReplicaChanges = delta.topicsDelta().localChanges(nodeId);
            collectGainedLeaderPartitions(localReplicaChanges, image, configuredMirrors, partitionsToTransition);
            cleanupLostLeaderPartitions(localReplicaChanges, image);
        }

        collectReconnectedMirrorPartitions(reconnectedMirrors, partitionsToTransition);
        return partitionsToTransition;
    }

    private void collectGainedLeaderPartitions(LocalReplicaChanges changes, MetadataImage image,
                                               Set<String> configuredMirrors, Set<TopicPartition> result) {
        changes.leaders().keySet().forEach(tp -> {
            String mirrorName = image.topics().getTopic(tp.topic()).mirrorName();
            if (mirrorName != null && configuredMirrors.contains(mirrorName)) {
                result.add(tp);
            }
        });
        changes.mirrorTopicStates().keySet().forEach(topicId -> {
            TopicImage topicImage = image.topics().getTopic(topicId);
            if (topicImage != null && topicImage.mirrorName() != null
                    && configuredMirrors.contains(topicImage.mirrorName())) {
                topicImage.partitions().forEach((partitionId, partition) -> {
                    if (partition.leader == nodeId) {
                        result.add(new TopicPartition(topicImage.name(), partitionId));
                    }
                });
            }
        });
    }

    private void cleanupLostLeaderPartitions(LocalReplicaChanges changes, MetadataImage image) {
        changes.followers().keySet().forEach(tp -> {
            String mirrorName = image.topics().getTopic(tp.topic()).mirrorName();
            if (mirrorName == null) {
                return;
            }
            mirrorCache.getPendingLederEpochBumps().removeIf(bump -> {
                bump.partitionToEpoch().remove(tp);
                if (bump.partitionToEpoch().isEmpty()) {
                    bump.future().complete(null);
                    return true;
                }
                return false;
            });
            if (!isLocalCoordinator(mirrorName, tp.topic(), tp.partition())) {
                MirrorPartitionKey key = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
                mirrorCache.removePartition(key);
            }
        });
    }

    private void collectReconnectedMirrorPartitions(Set<String> reconnectedMirrors, Set<TopicPartition> result) {
        if (reconnectedMirrors.isEmpty()) {
            return;
        }
        log.info("Re-evaluating MIRRORING partitions for reconnected mirrors: {}", reconnectedMirrors);
        mirrorCache.partitionKeys().forEach(key -> {
            MirrorPartition entry = mirrorCache.getPartition(key);
            if (entry != null && reconnectedMirrors.contains(key.mirrorName()) && entry.state() == MirrorPartitionState.MIRRORING) {
                metadataCache.getTopicName(key.topicId()).ifPresent(topicName ->
                        result.add(new TopicPartition(topicName, key.partition())));
            }
        });
    }

    /**
     * Full scan of all mirror leader partitions on this broker.
     * Called after coordinator shard loading completes to re-establish side effects.
     * Unlike {@code processStateTransitions(Set, MetadataImage)} which handles an
     * incremental delta, this method discovers every mirror partition led by this
     * broker from the current metadata image.
     */
    public void processAllStateTransitions() {
        if (!isInitialized || metadataImage == null) {
            return;
        }
        Set<TopicPartition> mirrorLeaders = new HashSet<>();
        metadataImage.topics().topicsByName().forEach((topicName, topicImage) -> {
            if (topicImage.mirrorName() != null) {
                topicImage.partitions().forEach((partitionId, partition) -> {
                    if (partition.leader == nodeId) {
                        mirrorLeaders.add(new TopicPartition(topicName, partitionId));
                    }
                });
            }
        });
        if (!mirrorLeaders.isEmpty()) {
            processStateTransitions(mirrorLeaders, metadataImage);
        }
    }

    /**
     * Applies state transitions for the given mirror partitions. Local coordinator
     * partitions transition inline; remote ones are batched by mirror and transitioned
     * after reading current state from the coordinator.
     */
    private void processStateTransitions(Set<TopicPartition> partitionsToTransition, MetadataImage newImage) {
        Map<String, Map<TopicPartition, Byte>> remoteDesiredStates = new HashMap<>();

        partitionsToTransition.forEach(tp -> {
            TopicImage topicImage = newImage.topics().getTopic(tp.topic());
            String mirrorName = topicImage.mirrorName();
            byte desiredMirrorState = topicImage.desiredMirrorState();
            boolean stopRequested = desiredMirrorState == MirrorPartitionState.STOPPED.value();
            boolean pauseRequested = desiredMirrorState == MirrorPartitionState.PAUSED.value();

            MirrorPartitionKey key = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
            if (isLocalCoordinator(key.mirrorName(), tp.topic(), tp.partition())) {
                MirrorPartition entry = mirrorCache.getPartition(key);
                MirrorPartitionState curState = entry != null ? entry.state() : MirrorPartitionState.UNKNOWN;
                log.debug("Local transition for {} (current: {})", tp, curState);
                applyStateTransition(key.mirrorName(), tp, curState, null, stopRequested, pauseRequested);
            } else {
                remoteDesiredStates
                        .computeIfAbsent(mirrorName, k -> new HashMap<>())
                        .put(tp, desiredMirrorState);
            }
        });

        remoteDesiredStates.forEach((mirrorName, desiredStates) -> {
            Map<String, Set<Integer>> partitions = new HashMap<>();
            desiredStates.keySet().forEach(tp ->
                    partitions.computeIfAbsent(tp.topic(), k -> new HashSet<>()).add(tp.partition()));
            log.debug("Reading remote coordinator state for mirror '{}': {}", mirrorName, partitions);
            readStateFromRemoteCoordinator(mirrorName, partitions, res ->
                    res.data().topics().forEach(topic ->
                            topic.partitions().forEach(partition -> {
                                if (partition.errorCode() != Errors.NONE.code()) {
                                    log.warn("Error reading mirror state for {}-{}: {}",
                                            topic.name(), partition.partitionIndex(), Errors.forCode(partition.errorCode()));
                                    return;
                                }
                                TopicPartition resTp = new TopicPartition(topic.name(), partition.partitionIndex());
                                MirrorPartitionState state = MirrorPartitionState.fromValue(partition.state());
                                byte desired = desiredStates.getOrDefault(resTp, MirrorPartitionState.UNKNOWN.value());
                                boolean stopRequested = desired == MirrorPartitionState.STOPPED.value();
                                boolean pauseRequested = desired == MirrorPartitionState.PAUSED.value();
                                MirrorPartitionKey mpk = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(resTp.topic()), resTp.partition());
                                MirrorPartition curEntry = mirrorCache.getPartition(mpk);
                                MirrorPartitionState curState = curEntry != null ? curEntry.state() : MirrorPartitionState.UNKNOWN;
                                applyStateTransition(mirrorName, resTp, curState, state, stopRequested, pauseRequested);
                            })));
        });
    }

    /** Completes epoch bump futures whose requested epochs are now reflected in the metadata image. */
    void maybeCompletePendingEpochBumps() {
        mirrorCache.getPendingLederEpochBumps().removeIf(bumpLeaderEpoch -> {
            Set<TopicPartition> pendingPartitions = bumpLeaderEpoch.partitionToEpoch().entrySet().stream().filter(entry -> {
                TopicPartition tp = entry.getKey();
                int epoch = entry.getValue();
                var topicImage = metadataImage.topics().getTopic(tp.topic());
                if (topicImage == null) return false;
                var partitionReg = topicImage.partitions().get(tp.partition());
                if (partitionReg == null) return false;
                return partitionReg.leaderEpoch <= epoch;
            }).map(Map.Entry::getKey).collect(Collectors.toSet());
            if (pendingPartitions.isEmpty()) {
                bumpLeaderEpoch.future().complete(null);
                return true;
            } else {
                log.info("bumpLeaderEpoch future is pending for partitions: {}, all: {}",
                        pendingPartitions, bumpLeaderEpoch.partitionToEpoch().keySet());
                return false;
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (mirrorStateSender != null) {
            mirrorStateSender.shutdown();
        }
        closeSourceAdmins();
        if (dstAdmin != null) {
            dstAdmin.close(Duration.ZERO);
        }
        mirrorCache.clear();
    }

    public void closeSourceAdmins() {
        if (srcAdmins != null) {
            srcAdmins.values().forEach(admin -> admin.close(Duration.ZERO));
        }
    }

    private void closeAndRemoveSourceAdmin(String mirrorName) {
        if (srcAdmins != null) {
            Admin admin = srcAdmins.remove(mirrorName);
            if (admin != null) {
                admin.close(Duration.ZERO);
            }
        }
    }

    /**
     * Applies the appropriate state transition based on current state and stop flag.
     * <p>
     * stopRequested: it means the partition should head to STOPPED state. When it is true (i.e. users stopMirrorTopics):
     *   1. if it's already in STOPPED state, then keep the state
     *   2. else, move the state to STOPPING state
     * <p>
     * pauseRequested: it means the partition should head to PAUSED state. When it is true (i.e. users pause it):
     *   1. if it's already in PAUSED state, then keep the state
     *   2. else, move the state to PAUSING state
     * <p>
     * When stopRequested=false and pauseRequested=false:
     *   1. if it's in PAUSED state, we should move it to MIRRORING state. It will happen when users resume mirroring
     *   2. if it's in UNKNOWN, STOPPED, or FAILED state, we should move it to LOG_TRUNCATION state.
     *      UNKNOWN/STOPPED happens on startMirrorTopics. FAILED happens on manual restart after retries are exhausted.
     *   3. else, keep the same state as is. This could happen like leadership change, and the new leader should
     *      continue to complete the process in previous leader
     * <p>
     * If the current state is FAILED, we only allow it to enter FAILED state because if we move the FAILED state based on
     * the "desired state", that means we ignore its previous state stored in MirrorPartition.
     * Ex: one partition failed when LOG_TRUNCATION. We should retry LOG_TRUNCATION until exhausted. But if we honor the
     * desired state, we might move this failed state into PAUSING or STOPPING state due to user's update.
     * This breaks the state machine diagram that a LOG_TRUNCATION state cannot move to PAUSING or STOPPING state.
     */
    private void applyStateTransition(String mirrorName, TopicPartition tp,
                                      MirrorPartitionState curState, MirrorPartitionState fetchedState,
                                      boolean stopRequested, boolean pauseRequested) {
        if (curState == MirrorPartitionState.FAILED) {
            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED);
        } else if (stopRequested) {
            if (curState != MirrorPartitionState.STOPPED) {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPING);
            } else {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPED);
            }
        } else if (pauseRequested) {
            if (curState != MirrorPartitionState.PAUSED) {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSING);
            } else {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED);
            }
        } else if (curState == MirrorPartitionState.PAUSED) {
            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING);
        } else if (curState == MirrorPartitionState.UNKNOWN
                || curState == MirrorPartitionState.STOPPED) {
            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.LOG_TRUNCATION);
        } else {
            transitionTo(mirrorName, Set.of(tp), fetchedState != null ? fetchedState : curState);
        }
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState state) {
        transitionTo(mirrorName, topicPartitions, state, null, false);
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState state, String errorMessage) {
        transitionTo(mirrorName, topicPartitions, state, errorMessage, false);
    }

    /**
     * Writes a state transition for each partition, routing to either the local coordinator
     * shard (via {@link CoreBridge.CoordinatorWriter}) or a remote coordinator (via
     * {@link #writeStateToRemoteCoordinator}). On successful write, dispatches side effects
     * through {@link #onStateTransition}.
     */
    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions,
                              MirrorPartitionState state, String errorMessage, boolean nonRetryable) {
        coordinatorWriter.ifPresent(writer -> {
            for (TopicPartition tp : topicPartitions) {
                MirrorPartitionState currentState = getPartitionState(mirrorName, tp);
                if (!MirrorPartitionState.isValidTransition(currentState, state)) {
                    log.warn("Skipping invalid transition from {} to {} for {}.", currentState, state, tp);
                    continue;
                }
                MirrorPartitionKey key = MirrorPartitionKey.of(
                        mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
                int stateEpoch = state == MirrorPartitionState.FAILED
                        ? -1 : MirrorPartition.orEmpty(mirrorCache.getPartition(key)).stateEpoch();
                if (isLocalCoordinator(mirrorName, tp.topic(), tp.partition())) {
                    writer.writePartitionState(mirrorName, tp, state, stateEpoch, errorMessage, nonRetryable)
                            .whenComplete((v, ex) -> {
                                if (ex != null) {
                                    Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                                            ? ex.getCause() : ex;
                                    if (cause instanceof CoordinatorLoadInProgressException) {
                                        log.debug("Transition to {} deferred for {} (shard loading).", state, tp);
                                        return;
                                    }
                                    if (cause instanceof FencedStateEpochException) {
                                        log.debug("Transition to {} fenced for {} (stale state epoch).", state, tp);
                                        return;
                                    }
                                    log.error("Transition to {} failed for {}", state, tp, ex);
                                    if (state != MirrorPartitionState.FAILED) {
                                        transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage());
                                    }
                                    return;
                                }
                                if (MirrorPartition.orEmpty(mirrorCache.getPartition(key)).state() == state) {
                                    onStateTransition(mirrorName, tp, state);
                                }
                            });
                } else {
                    Map<String, Set<MirrorStateWrite>> topicMetadata =
                            Map.of(tp.topic(), Set.of(new MirrorStateWrite(tp.partition(), state, stateEpoch, -1)));
                    writeStateToRemoteCoordinator(mirrorName, topicMetadata, Set.of(),
                            res -> res.data().topics().forEach(topic -> topic.partitions().forEach(par -> {
                                if (par.errorCode() == Errors.NONE.code()) {
                                    updateLocalFailedState(key, state, errorMessage, nonRetryable);
                                    mirrorCache.setPartition(key,
                                            MirrorPartition.orEmpty(mirrorCache.getPartition(key))
                                                    .withState(state)
                                                    .withStateEpoch(par.stateEpoch()));
                                    onStateTransition(mirrorName, tp, state);
                                } else if (par.errorCode() == Errors.FENCED_STATE_EPOCH.code()) {
                                    log.debug("Transition to {} fenced for {} (stale state epoch).", state, tp);
                                } else {
                                    log.error("Failed to write partition state to remote coordinator: {}",
                                            par.errorCode());
                                }
                            })));
                }
            }
        });
    }

    /**
     * Dispatches side effects after a coordinator write commits.
     * Each state triggers a specific action: LOG_TRUNCATION starts truncation,
     * EPOCH_FENCING bumps the leader epoch, MIRRORING creates fetchers,
     * PAUSING/STOPPING removes fetchers, FAILED schedules a retry.
     */
    private void onStateTransition(String mirrorName, TopicPartition tp, MirrorPartitionState newState) {
        switch (newState) {
            case LOG_TRUNCATION:
                scheduleTruncation(mirrorName, tp);
                break;
            case EPOCH_FENCING:
                scheduleBumpLeaderEpoch(mirrorName, tp)
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage());
                        } else {
                            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING);
                        }
                    });
                break;
            case MIRRORING:
                replicaManagerSupplier.get().maybeCreateMirrorFetchers(mirrorName, Set.of(tp));
                break;
            case PAUSING:
                replicaManagerSupplier.get().mirrorFetcherManager()
                    .removeFetcherForPartitions(CollectionConverters.asScala(Set.of(tp)));
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED);
                break;
            case STOPPING:
                handleStoppingTransition(mirrorName, tp);
                break;
            case PAUSED, STOPPED:
                break;
            case FAILED:
                scheduleFailedRetry(mirrorName, tp);
                break;
            default:
                throw new IllegalArgumentException("Illegal state transition to " + newState);
        }
    }

    /**
     * Handles the STOPPING lifecycle: removes fetchers, updates the last mirror epoch,
     * bumps the leader epoch, aborts ongoing transactions, writes PID reset barrier,
     * and finally transitions to STOPPED. On any failure, transitions to FAILED.
     */
    private void handleStoppingTransition(String mirrorName, TopicPartition tp) {
        ReplicaManager rm = replicaManagerSupplier.get();
        rm.mirrorFetcherManager().removeFetcherForPartitions(CollectionConverters.asScala(Set.of(tp)));
        var logOpt = rm.getPartitionOrException(tp).log();
        int latestEpoch = logOpt.isDefined() ? logOpt.get().latestEpoch().orElse(-1) : -1;
        updateLastMirrorEpoch(mirrorName, tp, latestEpoch)
            .thenCompose(v -> bumpLeaderEpochs(getLatestLocalEpoch(tp)))
            .thenCompose(v -> abortOngoingTransactions(tp))
            .thenCompose(v -> writePidResetBarrier(mirrorName, tp))
            .thenAccept(v -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPED))
            .exceptionally(ex -> {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage());
                return null;
            });
    }

    /** Updates the last mirror epoch in the local cache and persists it to the coordinator shard. */
    private CompletableFuture<Void> updateLastMirrorEpoch(String mirrorName, TopicPartition tp, int epoch) {
        if (epoch == -1) {
            return CompletableFuture.completedFuture(null);
        }
        setLastMirrorEpoch(mirrorName, tp.topic(), tp.partition(), epoch);
        if (isLocalCoordinator(mirrorName, tp.topic(), tp.partition())) {
            return coordinatorWriter.get().writeLastMirrorEpoch(mirrorName, tp, epoch);
        } else {
            writeStateToRemoteCoordinator(mirrorName,
                Map.of(tp.topic(), Set.of(new MirrorStateWrite(tp.partition(), null, -1, epoch))),
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
                    var sourceMirrors = listSourceClusterMirrors(mirrorName);
                    if (hasMirrorLoop(mirrorName, tp, sourceMirrors)) {
                        transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED,
                            "Detected mirror loop for mirror:" + mirrorName);
                        return;
                    }
                    sendLastMirrorEpochLookup(mirrorName, tp, sourceMirrors)
                        .whenComplete((epochs, rawError) -> {
                            if (rawError != null) {
                                Throwable error = rawError instanceof CompletionException && rawError.getCause() != null
                                    ? rawError.getCause() : rawError;
                                if (error instanceof UnsupportedVersionException) {
                                    log.warn("Source cluster doesn't support DescribeClusterMirror API. " +
                                        "Replication will be one-way without failback");
                                    replicaManagerSupplier.get().maybeTruncateForLeaderEpoch(
                                        Map.of(tp, -1), truncateCallback);
                                } else {
                                    log.warn("Failed to truncate to last mirrored epoch for mirror {}",
                                        mirrorName, error);
                                    transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED,
                                        error.getMessage());
                                }
                                return;
                            }
                            if (!epochs.containsKey(tp)) {
                                log.warn("No epoch returned for {}. Using -1.", tp);
                                epochs.put(tp, -1);
                            }
                            replicaManagerSupplier.get().maybeTruncateForLeaderEpoch(
                                epochs, truncateCallback);
                        });
                } catch (Exception e) {
                    log.warn("Failed to truncate to last mirror epochs for mirror {}", mirrorName, e);
                    transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, e.getMessage());
                }
            }, 0);
    }

    private void scheduleFailedRetry(String mirrorName, TopicPartition tp) {
        ClusterMirrorConfig mirrorConfig = brokerConfig.mirrorConfig();
        int maxAttempts = mirrorConfig.failedRetryMaxAttempts();
        MirrorPartitionKey key = MirrorPartitionKey.of(
            mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
        MirrorPartition mp = MirrorPartition.orEmpty(mirrorCache.getPartition(key));
        int attempt = mp.retryAttempt() != 0 ? mp.retryAttempt() : 1;
        if (attempt == MirrorPartition.NON_RETRYABLE_ATTEMPT) {
            log.debug("Skipping retry for partition {} (non-retryable failed state).", tp);
            return;
        }
        if (attempt >= maxAttempts) {
            log.error("Partition {} exceeded max retry attempts ({}), requires manual intervention.",
                tp, maxAttempts);
            return;
        }
        ExponentialBackoff backoff = new ExponentialBackoff(
            mirrorConfig.failedRetryInitialBackoffMs(),
            CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
            mirrorConfig.failedRetryMaxBackoffMs(),
            CommonClientConfigs.RETRY_BACKOFF_JITTER);
        long delay = backoff.backoff(attempt);
        MirrorPartitionState targetState = (mp.prevState() == null || mp.prevState() == MirrorPartitionState.UNKNOWN)
            ? MirrorPartitionState.LOG_TRUNCATION : mp.prevState();
        log.info("Scheduling retry #{} for {} in {} ms targeting {}.", attempt, tp, delay, targetState);
        scheduler.scheduleOnce("failed-retry-" + tp,
            () -> transitionTo(mirrorName, Set.of(tp), targetState), delay);
    }

    private CompletableFuture<Void> writePidResetBarrier(String mirrorName, TopicPartition tp) {
        String sourceClusterId = getSourceClusterId(mirrorName);
        if (sourceClusterId == null) {
            log.warn("Source cluster ID not available for mirror {}. Skipping PID reset barrier.", mirrorName);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        appendPidResetBarrier(tp, sourceClusterId, time.milliseconds())
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

    private void updateLocalFailedState(MirrorPartitionKey key, MirrorPartitionState newState,
                                        String errorMessage, boolean nonRetryable) {
        MirrorPartitionState curState = MirrorPartition.orEmpty(mirrorCache.getPartition(key)).state();
        mirrorCache.updateFailedInfo(key, curState, newState, errorMessage, nonRetryable);
    }

    private Map<TopicPartition, Integer> getLatestLocalEpoch(TopicPartition tp) {
        int epoch = replicaManagerSupplier.get().logManager().getLog(tp, false).get().latestEpoch().orElse(-1);
        return Map.of(tp, epoch);
    }

    /**
     * Writes tombstone records for all locally coordinated partitions of a deleted mirror,
     * then removes the mirror's cache entries. Called from {@link #handleMirrorConfigDeltas}
     * when a mirror config deletion is detected.
     */
    void tombstoneMirror(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> states = getMirrorStates(mirrorName);
        Map<Integer, Set<TopicPartition>> coordPartitionToMirrorPartitions = new HashMap<>();
        states.forEach((tp, state) -> {
            if (isLocalCoordinator(mirrorName, tp.topic(), tp.partition())) {
                coordPartitionToMirrorPartitions.computeIfAbsent(
                    coordPartFinder.get().apply(
                        MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())),
                    v -> new HashSet<>()).add(tp);
            }
        });

        mirrorCache.removeMirror(mirrorName);
        mirrorCache.clearPendingLeaderEpochBumps(states.keySet());

        if (coordPartitionToMirrorPartitions.isEmpty()) {
            states.keySet().forEach(tp ->
                mirrorCache.removePartition(
                    MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())));
            return;
        }

        for (Map.Entry<Integer, Set<TopicPartition>> entry : coordPartitionToMirrorPartitions.entrySet()) {
            Set<TopicPartition> tps = entry.getValue();
            coordinatorWriter.get().writeTombstone(mirrorName, tps)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("Failed to write tombstone for mirror '{}': {}. Will retry later.",
                            mirrorName, ex.getMessage());
                    } else {
                        tps.forEach(tp -> mirrorCache.removePartition(
                            MirrorPartitionKey.of(mirrorName,
                                metadataCache.getTopicId(tp.topic()), tp.partition())));
                    }
                });
        }
    }

    /**
     * Resolves the coordinator node for a mirror record key by hashing the key to a
     * {@code __mirror_state} partition and returning that partition's leader from the
     * local metadata cache. Returns {@link Node#noNode()} if the coordinator is unavailable.
     */
    private Node findCoordinatorNode(MirrorPartitionKey key) {
        try {
            if (coordPartFinder.isEmpty() || !metadataCache.contains(MIRROR_STATE_TOPIC_NAME)) {
                return Node.noNode();
            }

            var listenerName = brokerConfig.interBrokerListenerName();
            List<MetadataResponseData.MetadataResponseTopic> topicMetadata = metadataCache.getTopicMetadata(
                    Set.of(MIRROR_STATE_TOPIC_NAME), listenerName, false, false);

            if (topicMetadata == null || topicMetadata.isEmpty() || topicMetadata.get(0).errorCode() != Errors.NONE.code()) {
                return Node.noNode();
            }

            int partition = coordPartFinder.get().apply(key);
            return topicMetadata.get(0).partitions().stream()
                    .filter(p -> p.partitionIndex() == partition && p.leaderId() != MetadataResponse.NO_LEADER_ID)
                    .findFirst()
                    .flatMap(p -> metadataCache.getAliveBrokerNode(p.leaderId(), listenerName))
                    .orElse(Node.noNode());
        } catch (Exception e) {
            log.warn("Exception while getting mirror coordinator", e);
            return Node.noNode();
        }
    }

    /**
     * Reads partition states from remote coordinators, batching requests per coordinator node.
     * Updates the local {@link MirrorStateCache} with each response.
     */
    void readStateFromRemoteCoordinator(String mirrorName,
                                        Map<String, Set<Integer>> partitions,
                                        Consumer<ReadMirrorStatesResponse> callback) {
        log.debug("Reading states from remote coordinator: {} {}", mirrorName, partitions);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<ReadMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        partitions.forEach((topic, parts) -> {
            parts.forEach(part -> {
                MirrorPartitionKey key = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(topic), part);
                Node coordinatorNode = findCoordinatorNode(key);
                if (coordinatorNode.equals(Node.noNode())) {
                    log.warn("Coordinator is not available for mirror {} partition {}-{}", mirrorName, topic, part);
                    return;
                }

                ReadMirrorStatesRequestData.PartitionData partitionData = new ReadMirrorStatesRequestData.PartitionData();
                partitionData.setPartitionIndex(part);

                nodeToTopicPartitions
                        .computeIfAbsent(coordinatorNode, k -> new HashMap<>())
                        .computeIfAbsent(topic, k -> new ArrayList<>())
                        .add(partitionData);
            });
        });

        // Send one batched request per coordinator node
        nodeToTopicPartitions.forEach((node, topicPartitionsMap) -> {
            ReadMirrorStatesRequestData data = new ReadMirrorStatesRequestData().setMirrorName(mirrorName);
            List<ReadMirrorStatesRequestData.TopicMetadata> topicDataList = new ArrayList<>();

            topicPartitionsMap.forEach((topic, partitionDataList) ->
                    topicDataList.add(new ReadMirrorStatesRequestData.TopicMetadata()
                            .setName(topic)
                            .setPartitions(partitionDataList)));

            data.setTopics(topicDataList);

            mirrorStateSender.enqueue(new RequestAndCompletionHandler(
                    time.milliseconds(),
                    node,
                    new ReadMirrorStatesRequest.Builder(data),
                    response -> {
                        if (response.responseBody() instanceof ReadMirrorStatesResponse readMirrorStatesResponse) {
                            log.debug("Read states from remote coordinator completed: {}", response.responseBody());

                            readMirrorStatesResponse.data().topics().forEach(topic ->
                                topic.partitions().forEach(partition -> {
                                    MirrorPartitionKey mpk = MirrorPartitionKey.of(
                                            mirrorName, metadataCache.getTopicId(topic.name()), partition.partitionIndex());
                                    mirrorCache.mergePartition(mpk, partition.state(), partition.stateEpoch(),
                                            partition.lastMirrorEpoch(), partition.errorMessage(),
                                            partition.retryAttempt(), partition.previousState());
                                }));

                            callback.accept(readMirrorStatesResponse);
                        }
                    }
            ));
        });
    }

    /** Writes partition states to remote coordinators, batching requests per coordinator node. */
    public void writeStateToRemoteCoordinator(String mirrorName,
                                              Map<String, Set<MirrorStateWrite>> topicMetadata,
                                              Set<String> stoppedTopics,
                                              Consumer<WriteMirrorStatesResponse> callback) {
        log.debug("Writing states to remote coordinator: {} {} {}", mirrorName, topicMetadata, stoppedTopics);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<WriteMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        topicMetadata.forEach((topic, metadata) -> {
            metadata.forEach(m -> {
                MirrorPartitionKey key = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(topic), m.partition());
                Node coordinatorNode = findCoordinatorNode(key);
                if (coordinatorNode.equals(Node.noNode())) {
                    log.error("Coordinator is not available for mirror {} partition {}-{}", mirrorName, topic, m.partition());
                    return;
                }

                WriteMirrorStatesRequestData.PartitionData partitionData = new WriteMirrorStatesRequestData.PartitionData();
                partitionData.setState(m.state() == null ? MirrorPartitionState.UNKNOWN.value() : m.state().value());
                partitionData.setStateEpoch(m.stateEpoch());
                partitionData.setLastMirrorEpoch(m.leaderEpoch());
                partitionData.setPartitionIndex(m.partition());

                nodeToTopicPartitions
                    .computeIfAbsent(coordinatorNode, k -> new HashMap<>())
                    .computeIfAbsent(topic, k -> new ArrayList<>())
                    .add(partitionData);
            });
        });

        // Send one batched request per coordinator node
        nodeToTopicPartitions.forEach((node, topicPartitionsMap) -> {
            WriteMirrorStatesRequestData data = new WriteMirrorStatesRequestData().setMirrorName(mirrorName);
            List<WriteMirrorStatesRequestData.TopicMetadata> topicDataList = new ArrayList<>();

            topicPartitionsMap.forEach((topic, partitionDataList) ->
                topicDataList.add(new WriteMirrorStatesRequestData.TopicMetadata()
                    .setName(topic)
                    .setPartitions(partitionDataList)));

            data.setTopics(topicDataList);
            data.setStoppedTopics(new ArrayList<>(stoppedTopics));

            mirrorStateSender.enqueue(new RequestAndCompletionHandler(
                time.milliseconds(),
                node,
                new WriteMirrorStatesRequest.Builder(data),
                response -> {
                    log.debug("Write states to remote coordinator completed: {}", response.responseBody());
                    if (response.responseBody() instanceof WriteMirrorStatesResponse writeMirrorStatesResponse) {
                        callback.accept(writeMirrorStatesResponse);
                    }
                }
            ));
        });
    }

    public CompletableFuture<Void> abortOngoingTransactions(TopicPartition tp) {
        ReplicaManager rm = replicaManagerSupplier.get();
        var record = rm.getLog(tp).map(UnifiedLog::buildEndTransactionRecords);
        if (!record.isDefined() || record.get().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (MemoryRecords memRecords : record.get()) {
            CompletableFuture<Void> batchFuture = new CompletableFuture<>();
            rm.appendRecords(
                    5000L,
                    (short) -1,
                    true,
                    AppendOrigin.COORDINATOR,
                    CollectionConverters.asScala(Map.of(rm.topicIdPartition(tp), memRecords)),
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

    public CompletableFuture<Void> appendPidResetBarrier(TopicPartition tp, String sourceClusterId, long timestampMs) {
        ReplicaManager rm = replicaManagerSupplier.get();
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture<ProduceResponse.PartitionResponse> future = new CompletableFuture<>();
        MirrorPidResetRecord pidResetRecord = new MirrorPidResetRecord()
                .setVersion(ControlRecordUtils.MIRROR_PID_RESET_CURRENT_VERSION)
                .setSourceClusterId(sourceClusterId);
        try {
            var topicIdPartition = rm.topicIdPartition(tp);
            int bufferSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD + 256;
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            MemoryRecords records = MemoryRecords.withMirrorPidResetRecord(
                    0, timestampMs, 0, buffer, pidResetRecord);
            rm.appendRecords(
                    5000L,
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
            future.completeExceptionally(e);
        }
        future.whenComplete((pr, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else if (pr == null || pr.error.code() != 0) {
                String errorMsg = pr != null ? pr.error.message() : "no response";
                result.completeExceptionally(new RuntimeException(
                        "PID reset barrier error for " + tp + ": " + errorMsg));
            } else {
                result.complete(null);
            }
        });
        return result;
    }

    /**
     * Local-only LME lookup. Returns LME from the local coordinator cache
     * for partitions this broker coordinates, and -1 for the rest. The admin
     * client broadcasts DescribeClusterMirrors to all brokers and takes the
     * max, so each broker only needs its local view.
     *
     * @param mirrorPartitions mirrorName -> topicName -> partition indices
     * @return mirrorName -> (TopicPartition -> LME)
     */
    public Map<String, Map<TopicPartition, Integer>> processLastMirrorEpochLookup(
            Map<String, Map<String, Set<Integer>>> mirrorPartitions) {
        Map<String, Map<TopicPartition, Integer>> result = new HashMap<>();
        mirrorPartitions.forEach((mirrorName, topicParts) -> {
            topicParts.forEach((topic, parts) -> {
                parts.forEach(part -> {
                    MirrorPartitionKey pk = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(topic), part);
                    MirrorPartition cached = mirrorCache.getPartition(pk);
                    int lme = isLocalCoordinator(mirrorName, topic, part) && cached != null
                            ? cached.lastMirrorEpoch() : -1;
                    result.computeIfAbsent(mirrorName, k -> new HashMap<>())
                            .put(new TopicPartition(topic, part), lme);
                });
            });
        });
        return result;
    }

    public String getSourceClusterId(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        return (String) props.get(CommonClientConfigs.MIRROR_SOURCE_CLUSTER_ID_CONFIG);
    }

    public String getSourceBootstrap(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        return Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElse(null);
    }

    public Set<String> getConfiguredMirrors() {
        return metadataImage.configs().resourceData().keySet().stream()
                .filter(resource -> resource.type() == ConfigResource.Type.CLUSTER_MIRROR)
                .map(ConfigResource::name)
                .collect(Collectors.toSet());
    }

    public Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> result = new HashMap<>();
        mirrorCache.partitionKeys().forEach(key -> {
            if (key.mirrorName().equals(mirrorName)) {
                MirrorPartition entry = mirrorCache.getPartition(key);
                if (entry != null && entry.state() != null) {
                    metadataCache.getTopicName(key.topicId()).ifPresent(topicName ->
                            result.put(new TopicPartition(topicName, key.partition()), entry.state()));
                }
            }
        });
        return result;
    }

    public Set<String> getConfiguredTopics(String mirrorName, boolean includePaused) {
        return getConfiguredTopics(mirrorName, includePaused, true);
    }

    /**
     * Returns the set of topic names configured for the given mirror, filtered by desired state.
     *
     * @param mirrorName     the mirror name to look up
     * @param includePaused  whether to include topics in PAUSED state
     * @param includeStopped whether to include topics in STOPPED state
     * @return topic names matching the filter criteria
     */
    public Set<String> getConfiguredTopics(String mirrorName, boolean includePaused, boolean includeStopped) {
        return metadataImage.topics().topicsById().values().stream()
                .filter(topicInfo -> {
                    String topicMirrorName = topicInfo.mirrorName();
                    if (topicMirrorName == null || topicMirrorName.isBlank()) return false;
                    if (!mirrorName.equals(topicMirrorName)) return false;
                    byte state = topicInfo.desiredMirrorState();
                    if (!includeStopped && state == MirrorPartitionState.STOPPED.value()) return false;
                    return includePaused || state != MirrorPartitionState.PAUSED.value();
                })
                .map(TopicImage::name)
                .collect(Collectors.toSet());
    }

    public int getActiveTopicCount(String mirrorName) {
        return getConfiguredTopics(mirrorName, false, false).size();
    }

    public void validateDeleteMirrorStates(DeleteClusterMirrorRequestData data, Consumer<Optional<Errors>> callback) {
        Set<String> topics = getConfiguredTopics(data.mirrorName(), true, true);
        validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.STOPPED), false,
                data::setStateValidationOffset, callback);
    }

    public void validateStartMirrorStates(StartMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        Set<String> topics = data.topics().stream()
                .map(StartMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.STOPPED, MirrorPartitionState.UNKNOWN), true,
                data::setStateValidationOffset, callback);
    }

    public void validateStopMirrorStates(StopMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        Set<String> topics = data.topics().stream()
                .map(StopMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED), false,
                data::setStateValidationOffset, callback);
    }

    public void validatePauseMirrorStates(PauseMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        Set<String> topics = data.topics().stream()
                .map(PauseMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.MIRRORING), false,
                data::setStateValidationOffset, callback);
    }

    public void validateResumeMirrorStates(ResumeMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        Set<String> topics = data.topics().stream()
                .map(ResumeMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.PAUSED), false,
                data::setStateValidationOffset, callback);
    }

    /**
     * Validates partition states on the broker before forwarding a mirror operation to the controller.
     * Checks that both the desired state (from MetadataImage) and the actual coordinator state
     * (local cache + remote RPCs) are within {@code validStates}. On success, passes the metadata
     * offset at validation time to {@code offsetConsumer} so the caller can set it on the request
     * data. The controller uses this offset for optimistic locking, rejecting the request if any
     * mirror state changed after the broker's validation.
     *
     * @param mirrorName        the mirror being validated
     * @param topicNames        topic names whose partitions must be checked
     * @param validStates       the set of states that desired and actual partition states must belong to
     * @param skipMissingTopics if true, topics not yet in the metadata image are skipped (used by start)
     * @param offsetConsumer    receives the metadata offset on success so the caller can set it on the request
     * @param resultHandler     receives {@code Optional.empty()} on success, or an error on validation failure
     */
    private void validateMirrorStates(
            String mirrorName,
            Set<String> topicNames,
            Set<MirrorPartitionState> validStates,
            boolean skipMissingTopics,
            LongConsumer offsetConsumer,
            Consumer<Optional<Errors>> resultHandler) {
        MetadataImage currentImage = metadataImage;
        long validationOffset = currentImage.offset();
        Map<String, Set<Integer>> remotePartitions = new HashMap<>();

        Optional<Errors> localError = validateLocalPartitions(
                mirrorName, topicNames, validStates, skipMissingTopics, currentImage, remotePartitions);
        if (localError.isPresent()) {
            resultHandler.accept(localError);
            return;
        }

        if (remotePartitions.isEmpty()) {
            offsetConsumer.accept(validationOffset);
            resultHandler.accept(Optional.empty());
            return;
        }

        readStateFromRemoteCoordinator(mirrorName, remotePartitions, response -> {
            Optional<Errors> remoteError = validateRemotePartitions(response, validStates);
            if (remoteError.isPresent()) {
                resultHandler.accept(remoteError);
            } else {
                offsetConsumer.accept(validationOffset);
                resultHandler.accept(Optional.empty());
            }
        });
    }

    private Optional<Errors> validateLocalPartitions(
            String mirrorName,
            Set<String> topicNames,
            Set<MirrorPartitionState> validStates,
            boolean skipMissingTopics,
            MetadataImage currentImage,
            Map<String, Set<Integer>> remotePartitions) {
        Set<Byte> validDesiredStateValues = validStates.stream()
                .map(MirrorPartitionState::value).collect(Collectors.toSet());

        for (String topic : topicNames) {
            TopicImage topicImage = currentImage.topics().getTopic(topic);
            if (topicImage == null) {
                if (!skipMissingTopics) {
                    log.error("Topic {} not found in metadata image.", topic);
                    return Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATES);
                }
                continue;
            }
            if (!validDesiredStateValues.contains(topicImage.desiredMirrorState())) {
                log.error("Topic {} desired mirror state is {}, expected one of {}.",
                        topic, MirrorPartitionState.fromValue(topicImage.desiredMirrorState()), validStates);
                return Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATES);
            }
            for (int i = 0; i < topicImage.partitions().size(); i++) {
                if (isLocalCoordinator(mirrorName, topic, i)) {
                    MirrorPartition cachedEntry = mirrorCache.getPartition(
                            MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(topic), i));
                    MirrorPartitionState state = cachedEntry != null && cachedEntry.state() != null
                            ? cachedEntry.state() : MirrorPartitionState.UNKNOWN;
                    if (!validStates.contains(state)) {
                        log.error("Partition {}-{} is in {} state, expected one of {}.", topic, i, state, validStates);
                        return Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATES);
                    }
                } else if (metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME) != null) {
                    remotePartitions.computeIfAbsent(topic, k -> new HashSet<>()).add(i);
                } else {
                    log.info("Topic {} is not created completely. Mirror state is empty (UNKNOWN), passing validation.",
                            MIRROR_STATE_TOPIC_NAME);
                }
            }
        }
        return Optional.empty();
    }

    private Optional<Errors> validateRemotePartitions(
            ReadMirrorStatesResponse response,
            Set<MirrorPartitionState> validStates) {
        if (response.data().errorCode() != Errors.NONE.code()) {
            log.error("Error reading states from remote coordinator. Error code: {} and message: {}",
                    response.data().errorCode(), response.data().errorMessage());
            return Optional.of(Errors.forCode(response.data().errorCode()));
        }
        for (var topicResult : response.data().topics()) {
            for (var partitionResult : topicResult.partitions()) {
                if (partitionResult.errorCode() != Errors.NONE.code()) {
                    log.error("Error reading state from remote coordinator for partition {}-{}. Error code: {}",
                            topicResult.name(), partitionResult.partitionIndex(), partitionResult.errorCode());
                    return Optional.of(Errors.forCode(partitionResult.errorCode()));
                }
                MirrorPartitionState remoteState = MirrorPartitionState.fromValue(partitionResult.state());
                if (!validStates.contains(remoteState)) {
                    log.error("Remote partition {}-{} is in {} state, expected one of {}.",
                            topicResult.name(), partitionResult.partitionIndex(), remoteState, validStates);
                    return Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATES);
                }
            }
        }
        return Optional.empty();
    }

    // -- MirrorCache proxy --

    public MirrorPartition getPartition(MirrorPartitionKey key) {
        return mirrorCache.getPartition(key);
    }

    public MirrorPartitionState getPartitionState(String mirrorName, TopicPartition topicPartition) {
        MirrorPartition entry = mirrorCache.getPartition(
                MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(topicPartition.topic()), topicPartition.partition()));
        return entry != null ? entry.state() : null;
    }

    public void setPartition(MirrorPartitionKey key, MirrorPartition partition) {
        mirrorCache.setPartition(key, partition);
    }

    public void removePartition(MirrorPartitionKey key) {
        mirrorCache.removePartition(key);
    }

    public void clearPartition(int coordPartition, int coordPartitionCount) {
        mirrorCache.clearPartition(coordPartition, coordPartitionCount);
    }

    public void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch) {
        MirrorPartitionKey key = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(topic), partition);
        mirrorCache.setLastMirrorEpoch(key, epoch);
    }

    public void updateFailedInfo(MirrorPartitionKey key, MirrorPartitionState currentState,
                                 MirrorPartitionState newState, String errorMessage, boolean nonRetryable) {
        mirrorCache.updateFailedInfo(key, currentState, newState, errorMessage, nonRetryable);
    }

    public void clearFailedInfo(String mirrorName, TopicPartition tp) {
        mirrorCache.clearFailedInfo(MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition()));
    }

    public MirrorStateCache.SourceLeader resolveSourceLeader(String mirrorName, TopicPartition tp) {
        return mirrorCache.resolveSourceLeader(mirrorName, tp);
    }

    public void updateSourceLeader(String mirrorName, TopicPartition tp, MirrorStateCache.SourceLeader leader) {
        mirrorCache.updateSourceLeader(mirrorName, tp, leader);
    }

    // -- Source syncer proxy --

    public void scheduleMetadataRefresh(long intervalMs) {
        sourceSyncer.scheduleMetadataRefresh(intervalMs);
    }

    public void scheduleSourceTopicStateSync(String mirrorName) {
        sourceSyncer.scheduleSourceTopicStateSync(mirrorName);
    }

    boolean hasMirrorLoop(String mirrorName, TopicPartition tp,
                          Collection<ClusterMirrorListing> sourceMirrors) {
        return sourceSyncer.hasMirrorLoop(mirrorName, tp, sourceMirrors);
    }

    Collection<ClusterMirrorListing> listSourceClusterMirrors(String mirrorName) {
        return sourceSyncer.listSourceClusterMirrors(mirrorName);
    }

    public CompletionStage<Map<TopicPartition, Integer>> sendLastMirrorEpochLookup(
            String mirrorName, TopicPartition tp, Collection<ClusterMirrorListing> sourceMirrors) {
        return sourceSyncer.sendLastMirrorEpochLookup(mirrorName, tp, sourceMirrors);
    }

    public CompletableFuture<Void> scheduleBumpLeaderEpoch(String mirrorName, TopicPartition tp) {
        return sourceSyncer.scheduleBumpLeaderEpoch(mirrorName, tp);
    }

    public CompletableFuture<Void> bumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs) {
        return sourceSyncer.sendBumpLeaderEpochs(partitionMinEpochs);
    }
}
