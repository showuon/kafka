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

import kafka.log.LogManager;
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
import org.apache.kafka.common.message.DeleteClusterMirrorRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.PauseMirrorTopicsRequestData;
import org.apache.kafka.common.message.ReadMirrorStatesRequestData;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.ResumeMirrorTopicsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesRequest;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorService.MirrorStateWrite;
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
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import org.slf4j.Logger;

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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Central component for Cluster Mirroring on each broker.
 *
 * <p>Implements {@link MetadataPublisher} to react to KRaft leadership and config changes,
 * triggering mirror partition state transitions. Routes state reads and writes to the
 * appropriate coordinator broker via {@link MirrorStateSender}. Mirror partition state,
 * source leaders, and pending operations are held in a {@link MirrorStateCache}.
 *
 * <p>Periodic source cluster synchronization (topic metadata, configs, group offsets, ACLs,
 * pattern discovery, epoch bumping) is handled by {@link MirrorSourceSyncer}, which is
 * created during {@link #initialize} and accesses shared state through the cache.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class MirrorMetadataManager implements MetadataPublisher, AutoCloseable {
    private static final Set<String> NON_CONNECTION_CONFIGS = Set.of(
            ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, ClusterMirrorConfig.MIRROR_TOPICS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.MIRROR_GROUPS_INCLUDE_CONFIG, ClusterMirrorConfig.MIRROR_GROUPS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.MIRROR_ACL_INCLUDE_CONFIG);

    private final Logger log;
    private volatile boolean isInitialized = false;
    private final String name;
    private final String clusterId;
    private final KafkaConfig brokerConfig;
    private final int nodeId;

    private volatile MirrorSourceSyncer sourceSyncer;
    private final NodeToControllerChannelManager channelManager;
    private final Supplier<ReplicaManager> replicaManagerSupplier;
    private volatile MetadataImage metadataImage = MetadataImage.EMPTY;
    private final MetadataCache metadataCache;
    private final KafkaScheduler scheduler;
    private final Metrics metrics;
    private final Time time;
    private final MirrorStateCache cache;

    private volatile MirrorStateSender mirrorStateSender;
    private volatile Map<String, Admin> srcAdmins;
    private volatile Admin dstAdmin;

    private Optional<StateTransitioner> stateTransitioner = Optional.empty();
    Optional<Consumer<String>> tombstoneWriter = Optional.empty();
    private Optional<Function<MirrorPartitionKey, Integer>> coordPartitionFinderByKey = Optional.empty();
    private Optional<Function<String, Integer>> coordPartitionFinderByName = Optional.empty();

    // Metrics
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
        this.name = "[" + MirrorMetadataManager.class.getSimpleName() + " id=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(MirrorMetadataManager.class);
        this.nodeId = brokerConfig.nodeId();

        this.channelManager = channelManager;
        this.replicaManagerSupplier = replicaManagerSupplier;
        this.metadataCache = metadataCache;

        this.scheduler = scheduler;
        this.metrics = metrics;
        this.time = time;
        this.cache = MirrorStateCache.empty();

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
        metricsGroup.newGauge("LogTruncationPartitionState", () -> cache.partitionStateCount(MirrorPartitionState.LOG_TRUNCATION));
        metricsGroup.newGauge("EpochFencingPartitionState", () -> cache.partitionStateCount(MirrorPartitionState.EPOCH_FENCING));
        metricsGroup.newGauge("MirroringPartitionState", () -> cache.partitionStateCount(MirrorPartitionState.MIRRORING));
        metricsGroup.newGauge("PausingPartitionState", () -> cache.partitionStateCount(MirrorPartitionState.PAUSING));
        metricsGroup.newGauge("PausedPartitionState", () -> cache.partitionStateCount(MirrorPartitionState.PAUSED));
        metricsGroup.newGauge("StoppingPartitionState", () -> cache.partitionStateCount(MirrorPartitionState.STOPPING));
        metricsGroup.newGauge("StoppedPartitionState", () -> cache.partitionStateCount(MirrorPartitionState.STOPPED));
        metricsGroup.newGauge("FailedPartitionState", () -> cache.partitionStateCount(MirrorPartitionState.FAILED));
    }

    /**
     * Checks whether this broker leads the __mirror_state partition for the given mirror name.
     * Hashes by mirror name only, so all mirror-level work (source sync, config sync) is handled
     * by a single broker per mirror.
     */
    boolean isLocalCoordinator(String mirrorName) {
        if (coordPartitionFinderByName.isPresent() && metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME) != null) {
            int activeCoordinator = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordPartitionFinderByName.get().apply(mirrorName)).leader;
            return activeCoordinator == brokerConfig.nodeId();
        }
        return false;
    }

    /**
     * Checks whether this broker leads the __mirror_state partition for the given mirror partition.
     * Hashes by composite key (mirror name, topic id, partition), distributing partition-level
     * coordination across brokers so a mirror with many partitions does not bottleneck on one node.
     */
    private boolean isLocalCoordinator(String mirrorName, String topic, int partition) {
        if (metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME) != null && coordPartitionFinderByKey.isPresent()) {
            int activeCoordinator = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordPartitionFinderByKey.get().apply(
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
    public void initialize(StateTransitioner stateTransitioner,
                           Consumer<String> tombstoneWriter,
                           Function<MirrorPartitionKey, Integer> coordPartitionByKeyFinder,
                           Function<String, Integer> coordPartitionByNameFinder) {
        if (mirrorStateSender == null) {
            this.mirrorStateSender = new MirrorStateSender(MirrorStateSender.class.getSimpleName(),
                    NetworkUtils.buildNetworkClient(MirrorMetadataManager.class.getSimpleName(), brokerConfig, metrics, time, new LogContext(name())),
                    brokerConfig.requestTimeoutMs(), Time.SYSTEM);
            mirrorStateSender.start();
        }

        this.sourceSyncer = new MirrorSourceSyncer(brokerConfig, this, channelManager,
                metadataCache, scheduler, metadataRefreshError, topicConfigSyncError,
                consumerGroupOffsetSyncError, shareGroupOffsetSyncError, aclSyncError);
        sourceSyncer.scheduleMetadataRefresh(brokerConfig.mirrorConfig().metadataRefreshIntervalMs());

        this.stateTransitioner = Optional.of(stateTransitioner);
        this.tombstoneWriter = Optional.of(tombstoneWriter);
        this.coordPartitionFinderByKey = Optional.of(coordPartitionByKeyFinder);
        this.coordPartitionFinderByName = Optional.of(coordPartitionByNameFinder);

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

    public MirrorStateCache cache() {
        return cache;
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
                            tombstoneWriter.ifPresent(h -> h.accept(mirrorName));
                        }

                        boolean connectionConfigChanged = e.getValue().changes().keySet().stream()
                                .anyMatch(key -> !NON_CONNECTION_CONFIGS.contains(key));
                        if (connectionConfigChanged) {
                            log.info("Mirror '{}' has connection config changed. Recreating connections.", mirrorName);
                        }
                        if (connectionConfigChanged || mirrorDeleted) {
                            cache.removeSourceLeaders(mirrorName);
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

            // Phase 1: collect partitions where this broker gained mirror leadership
            localReplicaChanges.leaders().keySet().forEach(tp -> {
                String mirrorName = image.topics().getTopic(tp.topic()).mirrorName();
                if (mirrorName != null && configuredMirrors.contains(mirrorName)) {
                    partitionsToTransition.add(tp);
                }
            });
            localReplicaChanges.mirrorTopicStates().keySet().forEach(topicId -> {
                TopicImage topicImage = image.topics().getTopic(topicId);
                if (topicImage != null && topicImage.mirrorName() != null
                        && configuredMirrors.contains(topicImage.mirrorName())) {
                    topicImage.partitions().forEach((partitionId, partition) -> {
                        if (partition.leader == nodeId) {
                            partitionsToTransition.add(new TopicPartition(topicImage.name(), partitionId));
                        }
                    });
                }
            });

            // Phase 2: clean up state for partitions where this broker lost leadership
            localReplicaChanges.followers().keySet().forEach(tp -> {
                String mirrorName = image.topics().getTopic(tp.topic()).mirrorName();
                if (mirrorName == null) {
                    return;
                }
                cache.getPendingEpochBumps().removeIf(bump -> {
                    bump.partitionToEpoch().remove(tp);
                    if (bump.partitionToEpoch().isEmpty()) {
                        bump.future().complete(null);
                        return true;
                    }
                    return false;
                });
                if (!isLocalCoordinator(mirrorName, tp.topic(), tp.partition())) {
                    MirrorPartitionKey key = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
                    cache.remove(key);
                }
            });
        }

        // Phase 3: re-add MIRRORING partitions for mirrors whose source connection was recreated
        if (!reconnectedMirrors.isEmpty()) {
            log.info("Re-evaluating MIRRORING partitions for reconnected mirrors: {}", reconnectedMirrors);
            cache.forEach((key, entry) -> {
                if (reconnectedMirrors.contains(key.mirrorName()) && entry.state() == MirrorPartitionState.MIRRORING) {
                    metadataCache.getTopicName(key.topicId()).ifPresent(topicName ->
                            partitionsToTransition.add(new TopicPartition(topicName, key.partition())));
                }
            });
        }

        return partitionsToTransition;
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
                MirrorPartition entry = cache.get(key);
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
            readStatesFromRemoteCoordinator(mirrorName, partitions, res ->
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
                                MirrorPartition curEntry = cache.get(mpk);
                                MirrorPartitionState curState = curEntry != null ? curEntry.state() : MirrorPartitionState.UNKNOWN;
                                applyStateTransition(mirrorName, resTp, curState, state, stopRequested, pauseRequested);
                            })));
        });
    }

    /** Completes epoch bump futures whose requested epochs are now reflected in the metadata image. */
    void maybeCompletePendingEpochBumps() {
        cache.getPendingEpochBumps().removeIf(bumpLeaderEpoch -> {
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
                log.info("bumpLeaderEpoch future is pending for partitions: {}, all: {}", pendingPartitions, bumpLeaderEpoch.partitionToEpoch().keySet());
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
        cache.clear();
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

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartition, MirrorPartitionState state) {
        transitionTo(mirrorName, topicPartition, state, null, false);
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartition, MirrorPartitionState state, String errorMessage) {
        transitionTo(mirrorName, topicPartition, state, errorMessage, false);
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartition, MirrorPartitionState state, String errorMessage, boolean nonRetryable) {
        stateTransitioner.ifPresent(st -> st.transitionTo(mirrorName, topicPartition, state, errorMessage, nonRetryable));
    }

    public void scheduleSourceTopicStateSync(String mirrorName) {
        sourceSyncer.scheduleSourceTopicStateSync(mirrorName);
    }

    public boolean hasMirrorLoop(String mirrorName, TopicPartition tp,
                          Collection<ClusterMirrorListing> sourceMirrors) {
        return sourceSyncer.hasMirrorLoop(mirrorName, tp, sourceMirrors);
    }

    public Collection<ClusterMirrorListing> listSourceClusterMirrors(String mirrorName) {
        return sourceSyncer.listSourceClusterMirrors(mirrorName);
    }

    /**
     * Resolves the coordinator node for a mirror record key by hashing the key to a
     * {@code __mirror_state} partition and returning that partition's leader from the
     * local metadata cache. Returns {@link Node#noNode()} if the coordinator is unavailable.
     */
    private Node findCoordinatorNode(MirrorPartitionKey key) {
        try {
            if (coordPartitionFinderByKey.isEmpty() || !metadataCache.contains(MIRROR_STATE_TOPIC_NAME)) {
                return Node.noNode();
            }

            var listenerName = brokerConfig.interBrokerListenerName();
            List<MetadataResponseData.MetadataResponseTopic> topicMetadata = metadataCache.getTopicMetadata(
                    Set.of(MIRROR_STATE_TOPIC_NAME), listenerName, false, false);

            if (topicMetadata == null || topicMetadata.isEmpty() || topicMetadata.get(0).errorCode() != Errors.NONE.code()) {
                return Node.noNode();
            }

            int partition = coordPartitionFinderByKey.get().apply(key);
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
    void readStatesFromRemoteCoordinator(String mirrorName,
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
                                    cache.merge(mpk, partition.state(), partition.lastMirrorEpoch(),
                                            partition.errorMessage(), partition.retryAttempt(), partition.previousState());
                                }));

                            callback.accept(readMirrorStatesResponse);
                        }
                    }
            ));
        });
    }

    /** Writes partition states to remote coordinators, batching requests per coordinator node. */
    public void writeStatesToRemoteCoordinator(String mirrorName,
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

    /** Reads partition states and offsets from local cache. Used when this broker is the coordinator. */
    public void readMirrorStates(String mirrorName, Map<String, Set<Integer>> partitions,
                          Consumer<ReadMirrorStatesResponse> responseCallback) {
        ReadMirrorStatesResponseData data = new ReadMirrorStatesResponseData();
        List<ReadMirrorStatesResponseData.TopicResult> topicResults = new ArrayList<>();
        partitions.forEach((tp, parts) -> {
            ReadMirrorStatesResponseData.TopicResult topicResult = new ReadMirrorStatesResponseData.TopicResult().setName(tp);
            List<ReadMirrorStatesResponseData.PartitionResult> partitionResults = new ArrayList<>();
            parts.forEach(part -> {
                MirrorPartitionKey pk = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp), part);
                ReadMirrorStatesResponseData.PartitionResult partitionResult = new ReadMirrorStatesResponseData.PartitionResult();
                if (!isLocalCoordinator(mirrorName, tp, part)) {
                    partitionResult.setErrorCode(Errors.NOT_COORDINATOR.code());
                    partitionResult.setErrorMessage(Errors.NOT_COORDINATOR.message());
                } else {
                    MirrorPartition entry = cache.get(pk);
                    partitionResult.setPartitionIndex(part);
                    partitionResult.setLastMirrorEpoch(entry != null ? entry.lastMirrorEpoch() : -1);
                    MirrorPartitionState state = entry != null && entry.state() != null ? entry.state() : MirrorPartitionState.UNKNOWN;
                    partitionResult.setState(state.value());
                    partitionResult.setPreviousState(
                            entry != null && entry.prevState() != null ? entry.prevState().value() : MirrorPartitionState.UNKNOWN.value());
                    partitionResult.setRetryAttempt(entry != null ? (short) entry.retryAttempt() : (short) 0);
                    partitionResult.setErrorMessage(entry != null ? entry.errorMessage() : null);
                }
                partitionResults.add(partitionResult);
            });
            topicResult.setPartitions(partitionResults);
            topicResults.add(topicResult);
        });
        data.setTopics(topicResults);
        responseCallback.accept(new ReadMirrorStatesResponse(data));
    }

    /** Returns the cached partition state, or null if not tracked. */
    public MirrorPartitionState getPartitionState(String mirrorName, TopicPartition topicPartition) {
        MirrorPartition entry = cache.get(
                MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(topicPartition.topic()), topicPartition.partition()));
        return entry != null ? entry.state() : null;
    }

    public CompletableFuture<Void> scheduleBumpLeaderEpoch(String mirrorName, TopicPartition tp) {
        return sourceSyncer.scheduleBumpLeaderEpoch(mirrorName, tp);
    }

    public CompletableFuture<Void> bumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs) {
        return sourceSyncer.sendBumpLeaderEpochs(partitionMinEpochs);
    }

    public void scheduleMetadataRefresh(long intervalMs) {
        sourceSyncer.scheduleMetadataRefresh(intervalMs);
    }

    /** Returns the source cluster ID from the mirror config, or null if not yet resolved. */
    public String getSourceClusterId(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        return (String) props.get(CommonClientConfigs.MIRROR_SOURCE_CLUSTER_ID_CONFIG);
    }

    /** Returns the source bootstrap servers from the mirror config, or null if not set. */
    public String getSourceBootstrap(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        return Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElse(null);
    }

    /** Returns all mirror names present in the metadata image. */
    public Set<String> getConfiguredMirrors() {
        return metadataImage.configs().resourceData().keySet().stream()
                .filter(resource -> resource.type() == ConfigResource.Type.CLUSTER_MIRROR)
                .map(ConfigResource::name)
                .collect(Collectors.toSet());
    }

    /** Returns the cached partition states for all partitions of the given mirror. */
    public Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> result = new HashMap<>();
        cache.forEach((key, entry) -> {
            if (key.mirrorName().equals(mirrorName) && entry.state() != null) {
                metadataCache.getTopicName(key.topicId()).ifPresent(topicName ->
                        result.put(new TopicPartition(topicName, key.partition()), entry.state()));
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

    public Map<TopicPartition, Integer> getLatestLocalEpoch(LogManager logManager, TopicPartition tp) {
        int epoch = logManager.getLog(tp, false).get().latestEpoch().orElse(-1);
        return Map.of(tp, epoch);
    }

    public void clearFailedInfo(String mirrorName, TopicPartition tp) {
        cache.clearFailedInfo(MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition()));
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

        readStatesFromRemoteCoordinator(mirrorName, remotePartitions, response -> {
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
                    MirrorPartition cachedEntry = cache.get(
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

    public CompletionStage<Map<TopicPartition, Integer>> sendLastMirrorEpochLookup(
            String mirrorName, TopicPartition tp, Collection<ClusterMirrorListing> sourceMirrors) {
        return sourceSyncer.sendLastMirrorEpochLookup(mirrorName, tp, sourceMirrors);
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
                    MirrorPartition cached = cache.get(pk);
                    int lme = isLocalCoordinator(mirrorName, topic, part) && cached != null
                            ? cached.lastMirrorEpoch() : -1;
                    result.computeIfAbsent(mirrorName, k -> new HashMap<>())
                            .put(new TopicPartition(topic, part), lme);
                });
            });
        });
        return result;
    }

    public void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch) {
        MirrorPartitionKey key = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(topic), partition);
        cache.setLastMirrorEpoch(key, epoch);
    }

    /** Callback for partition state transitions triggered by the coordinator. */
    public interface StateTransitioner {
        void transitionTo(String mirrorName, Set<TopicPartition> topicPartition, MirrorPartitionState state, String errorMessage, boolean nonRetryable);
    }
}
