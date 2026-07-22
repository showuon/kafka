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
import kafka.server.mirror.ClusterMirrorUtils.FailedPartitionInfo;
import kafka.server.mirror.ClusterMirrorUtils.LeaderEpochBump;
import kafka.server.mirror.ClusterMirrorUtils.LeaderInfo;
import kafka.server.mirror.ClusterMirrorUtils.PartitionCacheEntry;
import kafka.server.mirror.ClusterMirrorUtils.StateTransitioner;

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
import org.apache.kafka.coordinator.mirror.ClusterMirrorRecordKey;
import org.apache.kafka.image.LocalReplicaChanges;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.config.ClusterMirrorConfig;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
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
 * triggering mirror partition state transitions (LOG_TRUNCATION, EPOCH_FENCING, MIRRORING,
 * PAUSING, STOPPING, etc.). Routes state reads and writes to the appropriate coordinator
 * broker via {@link MirrorStateSender}.
 *
 * <p>Periodic source cluster synchronization (topic metadata, configs, group offsets, ACLs,
 * pattern discovery, epoch bumping) is handled by {@link MirrorSourceSyncer}, which is
 * created during {@link #initialize} and accesses shared state through this manager.
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

    // Network communication
    private volatile MirrorStateSender mirrorStateSender;
    private final Map<String, Admin> srcAdmins = new ConcurrentHashMap<>();
    private volatile Admin dstAdmin;

    // Local cache
    final Map<String, Map<TopicPartition, LeaderInfo>> sourceLeaders = new ConcurrentHashMap<>();
    final Map<ClusterMirrorRecordKey, PartitionCacheEntry> partitionCache = new ConcurrentHashMap<>();
    final Set<String> pendingTopicCreations = ConcurrentHashMap.newKeySet();
    private final Map<TopicPartition, MirrorPartitionState> pendingPartitionStates = new ConcurrentHashMap<>();
    final Set<LeaderEpochBump> pendingLeaderEpochBumps = ConcurrentHashMap.newKeySet();
    final Map<TopicPartition, ScheduledFuture<?>> pendingRetryFutures = new ConcurrentHashMap<>();

    // Functions
    private Optional<StateTransitioner> stateTransitioner = Optional.empty();
    Optional<Consumer<String>> tombstoneWriter = Optional.empty();
    private Optional<Function<ClusterMirrorRecordKey, Integer>> coordPartitionFinderByKey = Optional.empty();
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
        metricsGroup.newGauge("LogTruncationPartitionState", () -> partitionStateCount(MirrorPartitionState.LOG_TRUNCATION));
        metricsGroup.newGauge("EpochFencingPartitionState", () -> partitionStateCount(MirrorPartitionState.EPOCH_FENCING));
        metricsGroup.newGauge("MirroringPartitionState", () -> partitionStateCount(MirrorPartitionState.MIRRORING));
        metricsGroup.newGauge("PausingPartitionState", () -> partitionStateCount(MirrorPartitionState.PAUSING));
        metricsGroup.newGauge("PausedPartitionState", () -> partitionStateCount(MirrorPartitionState.PAUSED));
        metricsGroup.newGauge("StoppingPartitionState", () -> partitionStateCount(MirrorPartitionState.STOPPING));
        metricsGroup.newGauge("StoppedPartitionState", () -> partitionStateCount(MirrorPartitionState.STOPPED));
        metricsGroup.newGauge("FailedPartitionState", () -> partitionStateCount(MirrorPartitionState.FAILED));
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
                            ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topic), partition))).leader;
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
                           Function<ClusterMirrorRecordKey, Integer> coordPartitionByKeyFinder,
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
    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (!isInitialized) {
            return;
        }

        this.metadataImage = newImage;

        Set<String> reconnectedMirrors = maybeRecreateSourceConnection(delta, newImage);
        Set<TopicPartition> mirrorLeaders = collectMirrorLeaderChanges(delta, newImage, reconnectedMirrors);

        if (mirrorLeaders.isEmpty()) {
            return;
        }

        log.info("Processing metadata update for {} mirror leader partition(s): {}", mirrorLeaders.size(), mirrorLeaders);

        processStateTransitions(mirrorLeaders, newImage);
        maybeCompletePendingEpochBumps();
    }

    /**
     * Handles mirror config changes from the metadata delta. When a connection config changes
     * (e.g. bootstrap.servers), closes the old source AdminClient, removes fetchers, and eagerly
     * fetches source metadata so fetchers can be recreated immediately. When a mirror is deleted,
     * writes tombstone records and cleans up all cached state.
     *
     * @return names of mirrors whose source connections were recreated (excludes deleted mirrors)
     */
    private Set<String> maybeRecreateSourceConnection(MetadataDelta delta, MetadataImage newImage) {
        Set<String> reconnectedMirrors = new HashSet<>();
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
                            sourceLeaders.remove(mirrorName);
                            Admin admin = srcAdmins.remove(mirrorName);
                            if (admin != null) {
                                admin.close(Duration.ZERO);
                            }
                            var mirrorFetcherManager = replicaManagerSupplier.get().mirrorFetcherManager();
                            mirrorFetcherManager.removeFetchersForMirror(mirrorName);
                            mirrorFetcherManager.shutdownIdleFetcherThreads();
                            if (!mirrorDeleted) {
                                reconnectedMirrors.add(mirrorName);
                            }
                        }
                    });
        }
        return reconnectedMirrors;
    }

    /**
     * Collects mirror partitions that need state transitions and cleans up state for lost leadership.
     * Three phases:
     * 1. Collect new mirror leaders from the topics delta (leadership gains and mirror state changes)
     * 2. Clean up cached state for partitions where this broker lost leadership
     * 3. Re-add MIRRORING partitions for reconnected mirrors so their fetchers get recreated
     */
    private Set<TopicPartition> collectMirrorLeaderChanges(MetadataDelta delta, MetadataImage image,
                                                           Set<String> reconnectedMirrors) {
        Set<TopicPartition> mirrorLeaderPartitions = new HashSet<>();

        if (delta.topicsDelta() != null) {
            LocalReplicaChanges localReplicaChanges = delta.topicsDelta().localChanges(nodeId);

            // Phase 1: collect partitions where this broker gained mirror leadership
            localReplicaChanges.leaders().keySet().forEach(tp -> {
                String mirrorName = image.topics().getTopic(tp.topic()).mirrorName();
                if (mirrorName != null) {
                    mirrorLeaderPartitions.add(tp);
                }
            });
            localReplicaChanges.mirrorTopicStates().keySet().forEach(topicId -> {
                TopicImage topicImage = image.topics().getTopic(topicId);
                if (topicImage != null) {
                    topicImage.partitions().forEach((partitionId, partition) -> {
                        if (partition.leader == nodeId) {
                            mirrorLeaderPartitions.add(new TopicPartition(topicImage.name(), partitionId));
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
                pendingPartitionStates.remove(tp);
                removePendingRetryFuture(tp);
                pendingLeaderEpochBumps.removeIf(bump -> {
                    bump.partitionToEpoch().remove(tp);
                    if (bump.partitionToEpoch().isEmpty()) {
                        bump.future().complete(null);
                        return true;
                    }
                    return false;
                });
                if (!isLocalCoordinator(mirrorName, tp.topic(), tp.partition())) {
                    ClusterMirrorRecordKey key = ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
                    removePartitionState(key);
                }
            });
        }

        // Phase 3: re-add MIRRORING partitions for mirrors whose source connection was recreated
        if (!reconnectedMirrors.isEmpty()) {
            log.info("Re-evaluating MIRRORING partitions for reconnected mirrors: {}", reconnectedMirrors);
            partitionCache.forEach((key, entry) -> {
                if (reconnectedMirrors.contains(key.mirrorName()) && entry.state() == MirrorPartitionState.MIRRORING) {
                    metadataCache.getTopicName(key.topicId()).ifPresent(topicName ->
                            mirrorLeaderPartitions.add(new TopicPartition(topicName, key.partition())));
                }
            });
        }

        return mirrorLeaderPartitions;
    }

    /**
     * Applies state transitions for mirror leader partitions. Local coordinator partitions are handled
     * inline. Remote coordinator partitions are grouped by mirror for batched reads, then transitions
     * are applied from the responses.
     */
    private void processStateTransitions(Set<TopicPartition> mirrorLeaders, MetadataImage newImage) {
        Map<String, Map<TopicPartition, Byte>> remoteDesiredStates = new HashMap<>();

        mirrorLeaders.forEach(tp -> {
            TopicImage topicImage = newImage.topics().getTopic(tp.topic());
            String mirrorName = topicImage.mirrorName();
            byte desiredMirrorState = topicImage.desiredMirrorState();
            boolean stopRequested = desiredMirrorState == MirrorPartitionState.STOPPED.value();
            boolean pauseRequested = desiredMirrorState == MirrorPartitionState.PAUSED.value();

            ClusterMirrorRecordKey key = ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
            if (isLocalCoordinator(key.mirrorName(), tp.topic(), tp.partition())) {
                PartitionCacheEntry entry = partitionCache.get(key);
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
                                ClusterMirrorRecordKey mpk = ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(resTp.topic()), resTp.partition());
                                PartitionCacheEntry curEntry = partitionCache.get(mpk);
                                MirrorPartitionState curState = curEntry != null ? curEntry.state() : MirrorPartitionState.UNKNOWN;
                                applyStateTransition(mirrorName, resTp, curState, state, stopRequested, pauseRequested);
                            })));
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
        clearCache();
    }

    // Force-close source admin clients so any in-flight requests fail immediately
    // instead of blocking until requestTimeoutMs expires.
    void closeSourceAdmins() {
        srcAdmins.values().forEach(admin -> admin.close(Duration.ZERO));
    }

    void clearCache() {
        partitionCache.clear();
        sourceLeaders.clear();
        pendingLeaderEpochBumps.clear();
        pendingPartitionStates.clear();
        removePendingRetryFutures();
    }

    private void removePendingRetryFutures() {
        pendingRetryFutures.values().forEach(f -> f.cancel(false));
        pendingRetryFutures.clear();
    }

    private void removePendingRetryFuture(TopicPartition tp) {
        var future = pendingRetryFutures.remove(tp);
        if (future != null)
            future.cancel(false);
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
     * the "desired state", that means we ignore its previous state stored in FailedPartitionInfo.
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

    boolean hasMirrorLoop(String mirrorName, Set<TopicPartition> topicPartitions,
                          Collection<ClusterMirrorListing> sourceMirrors) {
        return sourceSyncer.hasMirrorLoop(mirrorName, topicPartitions, sourceMirrors);
    }

    Collection<ClusterMirrorListing> listSourceClusterMirrors(String mirrorName) {
        return sourceSyncer.listSourceClusterMirrors(mirrorName);
    }

    /**
     * Resolves the coordinator node for a mirror record key by hashing the key to a
     * {@code __mirror_state} partition and returning that partition's leader from the
     * local metadata cache. Returns {@link Node#noNode()} if the coordinator is unavailable.
     */
    private Node findCoordinatorNode(ClusterMirrorRecordKey key) {
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
     * Updates local cache (partitionCache) with each response.
     */
    void readStatesFromRemoteCoordinator(String mirrorName,
                                         Map<String, Set<Integer>> partitions,
                                         Consumer<ReadMirrorStatesResponse> callback) {
        log.debug("Reading states from remote coordinator: {} {}", mirrorName, partitions);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<ReadMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        partitions.forEach((topic, parts) -> {
            parts.forEach(part -> {
                ClusterMirrorRecordKey key = ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topic), part);
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

                            readMirrorStatesResponse.data().topics().forEach(topic -> {
                                topic.partitions().forEach(partition -> {
                                    ClusterMirrorRecordKey mpk = ClusterMirrorRecordKey.of(
                                            mirrorName, metadataCache.getTopicId(topic.name()), partition.partitionIndex());
                                    partitionCache.compute(mpk, (k, existing) -> {
                                        MirrorPartitionState state = existing != null ? existing.state() : MirrorPartitionState.UNKNOWN;
                                        int epoch = existing != null ? existing.lastMirrorEpoch() : -1;
                                        FailedPartitionInfo fpi = existing != null ? existing.failedInfo() : null;
                                        if (partition.lastMirrorEpoch() != -1) {
                                            epoch = partition.lastMirrorEpoch();
                                        }
                                        if (partition.state() != -1) {
                                            state = MirrorPartitionState.fromValue(partition.state());
                                        }
                                        if (partition.state() == MirrorPartitionState.FAILED.value()) {
                                            fpi = new FailedPartitionInfo(
                                                    partition.retryAttempt(), partition.errorMessage(),
                                                    MirrorPartitionState.fromValue(partition.previousState()));
                                        }
                                        return new PartitionCacheEntry(state, epoch, fpi);
                                    });
                                });
                            });

                            callback.accept(readMirrorStatesResponse);
                        }
                    }
            ));
        });
    }

    /** Writes partition states to remote coordinators, batching requests per coordinator node. */
    void writeStatesToRemoteCoordinator(String mirrorName,
                                        Map<String, Set<ClusterMirrorUtils.PartitionStateInfo>> topicMetadata,
                                        Set<String> stoppedTopics,
                                        Consumer<WriteMirrorStatesResponse> callback) {
        log.debug("Writing states to remote coordinator: {} {} {}", mirrorName, topicMetadata, stoppedTopics);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<WriteMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        topicMetadata.forEach((topic, metadata) -> {
            metadata.forEach(m -> {
                ClusterMirrorRecordKey key = ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topic), m.partition());
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
    void getTopicMetadata(String mirrorName, Map<String, Set<Integer>> partitions,
                          Consumer<ReadMirrorStatesResponse> responseCallback) {
        ReadMirrorStatesResponseData data = new ReadMirrorStatesResponseData();
        List<ReadMirrorStatesResponseData.TopicResult> topicResults = new ArrayList<>();
        partitions.forEach((tp, parts) -> {
            ReadMirrorStatesResponseData.TopicResult topicResult = new ReadMirrorStatesResponseData.TopicResult().setName(tp);
            List<ReadMirrorStatesResponseData.PartitionResult> partitionResults = new ArrayList<>();
            parts.forEach(part -> {
                ClusterMirrorRecordKey pk = ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp), part);
                ReadMirrorStatesResponseData.PartitionResult partitionResult = new ReadMirrorStatesResponseData.PartitionResult();
                if (!isLocalCoordinator(mirrorName, tp, part)) {
                    partitionResult.setErrorCode(Errors.NOT_COORDINATOR.code());
                    partitionResult.setErrorMessage(Errors.NOT_COORDINATOR.message());
                } else {
                    PartitionCacheEntry entry = partitionCache.get(pk);
                    partitionResult.setPartitionIndex(part);
                    partitionResult.setLastMirrorEpoch(entry != null ? entry.lastMirrorEpoch() : -1);
                    MirrorPartitionState state = entry != null && entry.state() != null ? entry.state() : MirrorPartitionState.UNKNOWN;
                    partitionResult.setState(state.value());
                    FailedPartitionInfo fpi = entry != null ? entry.failedInfo() : null;
                    partitionResult.setPreviousState(
                            fpi != null ? fpi.previousState().value() : MirrorPartitionState.UNKNOWN.value());
                    partitionResult.setRetryAttempt(fpi != null ? (short) fpi.retryAttempt() : (short) 0);
                    partitionResult.setErrorMessage(fpi != null ? fpi.errorMessage() : null);
                }
                partitionResults.add(partitionResult);
            });
            topicResult.setPartitions(partitionResults);
            topicResults.add(topicResult);
        });
        data.setTopics(topicResults);
        responseCallback.accept(new ReadMirrorStatesResponse(data));
    }

    /** Updates cached source leader for a specific partition. */
    public void updateSourceLeader(String mirrorName, TopicPartition tp, LeaderInfo leader) {
        sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>()).put(tp, leader);
    }

    /** Resolves the cached source leader for a partition, throwing if no metadata is available. */
    public LeaderInfo resolveSourceLeader(String mirrorName, TopicPartition tp) {
        var partitionLeaders = sourceLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            LeaderInfo leader = partitionLeaders.get(tp);
            if (leader != null) {
                return leader;
            }
        }
        throw new IllegalStateException("No source cluster metadata available for mirror " + mirrorName + " partition:" + tp);
    }

    void updatePartitionState(ClusterMirrorRecordKey key, MirrorPartitionState newState) {
        partitionCache.compute(key, (k, existing) -> {
            int epoch = existing != null ? existing.lastMirrorEpoch() : -1;
            FailedPartitionInfo fpi = existing != null ? existing.failedInfo() : null;
            return new PartitionCacheEntry(newState, epoch, fpi);
        });
    }

    void removePartitionState(ClusterMirrorRecordKey key) {
        partitionCache.remove(key);
    }

    void removeCachedMirror(String mirrorName) {
        partitionCache.keySet().removeIf(key -> key.mirrorName().equals(mirrorName));
    }

    void removeStateForPartitions(Set<TopicPartition> partitions) {
        partitions.forEach(pendingPartitionStates::remove);
        pendingLeaderEpochBumps.removeIf(bump -> {
            bump.partitionToEpoch().keySet().removeAll(partitions);
            if (bump.partitionToEpoch().isEmpty()) {
                bump.future().cancel(false);
                return true;
            }
            return false;
        });
    }

    private long partitionStateCount(MirrorPartitionState state) {
        return partitionCache.values().stream()
                .filter(entry -> entry.state() == state)
                .count();
    }

    /** Returns the cached partition state, or null if not tracked. */
    public MirrorPartitionState getPartitionState(String mirrorName, TopicPartition topicPartition) {
        PartitionCacheEntry entry = partitionCache.get(
                ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topicPartition.topic()), topicPartition.partition()));
        return entry != null ? entry.state() : null;
    }

    /** Delegates to {@link MirrorSourceSyncer#scheduleBumpLeaderEpochs}. */
    public CompletableFuture<Void> scheduleBumpLeaderEpochs(String mirrorName, Set<TopicPartition> topicPartitions) {
        return sourceSyncer.scheduleBumpLeaderEpochs(mirrorName, topicPartitions);
    }

    /** Delegates to {@link MirrorSourceSyncer#sendBumpLeaderEpochs}. */
    public CompletableFuture<Void> sendBumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs) {
        return sourceSyncer.sendBumpLeaderEpochs(partitionMinEpochs);
    }

    void maybeCompletePendingEpochBumps() {
        pendingLeaderEpochBumps.removeIf(bumpLeaderEpoch -> {
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

    /** Delegates to {@link MirrorSourceSyncer#scheduleMetadataRefresh}. */
    void scheduleMetadataRefresh(long intervalMs) {
        sourceSyncer.scheduleMetadataRefresh(intervalMs);
    }

    /** Returns the source cluster ID from the mirror config, or null if not yet resolved. */
    String getSourceClusterId(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        return (String) props.get(CommonClientConfigs.MIRROR_SOURCE_CLUSTER_ID_CONFIG);
    }

    /** Returns the source bootstrap servers from the mirror config, or null if not set. */
    String getSourceBootstrap(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        return Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElse(null);
    }

    /** Returns all mirror names present in the metadata image. */
    Set<String> getConfiguredMirrors() {
        return metadataImage.configs().resourceData().keySet().stream()
                .filter(resource -> resource.type() == ConfigResource.Type.CLUSTER_MIRROR)
                .map(ConfigResource::name)
                .collect(Collectors.toSet());
    }

    /** Returns the cached partition states for all partitions of the given mirror. */
    Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> result = new HashMap<>();
        partitionCache.forEach((key, entry) -> {
            if (key.mirrorName().equals(mirrorName) && entry.state() != null) {
                metadataCache.getTopicName(key.topicId()).ifPresent(topicName ->
                        result.put(new TopicPartition(topicName, key.partition()), entry.state()));
            }
        });
        return result;
    }

    Set<String> getConfiguredTopics(String mirrorName, boolean includePaused) {
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
    Set<String> getConfiguredTopics(String mirrorName, boolean includePaused, boolean includeStopped) {
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

    int getActiveTopicCount(String mirrorName) {
        return getConfiguredTopics(mirrorName, false, false).size();
    }

    /** Reads the latest epoch from local logs for each partition. */
    public Map<TopicPartition, Integer> getLatestLocalEpochs(LogManager logManager, Set<TopicPartition> topicPartitions) {
        Map<TopicPartition, Integer> partitionMinEpochs = new HashMap<>();
        topicPartitions.forEach(tp -> {
            int epoch = logManager.getLog(tp, false).get().latestEpoch().orElse(-1);
            partitionMinEpochs.put(tp, epoch);
        });
        return partitionMinEpochs;
    }

    public FailedPartitionInfo getFailedInfo(ClusterMirrorRecordKey key) {
        PartitionCacheEntry entry = partitionCache.get(key);
        return entry != null ? entry.failedInfo() : null;
    }

    public void setFailedInfo(ClusterMirrorRecordKey key, FailedPartitionInfo info) {
        partitionCache.compute(key, (k, existing) -> {
            MirrorPartitionState state = existing != null ? existing.state() : null;
            int epoch = existing != null ? existing.lastMirrorEpoch() : -1;
            return new PartitionCacheEntry(state, epoch, info);
        });
    }

    public void computeFailedInfo(ClusterMirrorRecordKey key, BiFunction<ClusterMirrorRecordKey, FailedPartitionInfo, FailedPartitionInfo> remapper) {
        partitionCache.compute(key, (k, existing) -> {
            FailedPartitionInfo oldInfo = existing != null ? existing.failedInfo() : null;
            FailedPartitionInfo newInfo = remapper.apply(k, oldInfo);
            MirrorPartitionState state = existing != null ? existing.state() : null;
            int epoch = existing != null ? existing.lastMirrorEpoch() : -1;
            return new PartitionCacheEntry(state, epoch, newInfo);
        });
    }

    public void clearFailedInfo(ClusterMirrorRecordKey key) {
        partitionCache.computeIfPresent(key, (k, existing) ->
                new PartitionCacheEntry(existing.state(), existing.lastMirrorEpoch(), null));
        metadataCache.getTopicName(key.topicId()).ifPresent(topicName ->
                removePendingRetryFuture(new TopicPartition(topicName, key.partition())));
    }

    public void clearFailedInfo(String mirrorName, TopicPartition tp) {
        clearFailedInfo(ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition()));
    }

    public Map<TopicPartition, MirrorPartitionState> pendingPartitionStates() {
        return pendingPartitionStates;
    }

    /** Groups replayed partition states by mirror and state, then invokes the callback for each group where this broker is the partition leader. */
    void applyReplayedStates(ClusterMirrorUtils.StateTransitionCallback callback) {
        Map<String, Map<MirrorPartitionState, Set<TopicPartition>>> transitionsByMirror = new HashMap<>();
        partitionCache.forEach((key, entry) -> {
            MirrorPartitionState state = entry.state();
            if (state == null) return;
            metadataCache.getTopicName(key.topicId()).ifPresent(topicName -> {
                metadataCache.getLeaderAndIsr(topicName, key.partition()).ifPresent(leaderAndIsr -> {
                    if (leaderAndIsr.leader() != nodeId) return;
                    TopicPartition tp = new TopicPartition(topicName, key.partition());
                    transitionsByMirror
                            .computeIfAbsent(key.mirrorName(), k -> new HashMap<>())
                            .computeIfAbsent(state, s -> new HashSet<>())
                            .add(tp);
                });
            });
        });

        transitionsByMirror.forEach((mirrorName, stateToPartitions) ->
                stateToPartitions.forEach((state, partitions) ->
                        callback.onStateReplayed(mirrorName, partitions, state)));
    }

    void validateDeleteMirrorStates(DeleteClusterMirrorRequestData data, Consumer<Optional<Errors>> callback) {
        Set<String> topics = getConfiguredTopics(data.mirrorName(), true, true);
        validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.STOPPED), false,
                data::setStateValidationOffset, callback);
    }

    void validateStartMirrorStates(StartMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        Set<String> topics = data.topics().stream()
                .map(StartMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.STOPPED, MirrorPartitionState.UNKNOWN), true,
                data::setStateValidationOffset, callback);
    }

    void validateStopMirrorStates(StopMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        Set<String> topics = data.topics().stream()
                .map(StopMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED), false,
                data::setStateValidationOffset, callback);
    }

    void validatePauseMirrorStates(PauseMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        Set<String> topics = data.topics().stream()
                .map(PauseMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.MIRRORING), false,
                data::setStateValidationOffset, callback);
    }

    void validateResumeMirrorStates(ResumeMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
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
                    PartitionCacheEntry cachedEntry = partitionCache.get(
                            ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topic), i));
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

    CompletionStage<Map<TopicPartition, Integer>> sendLastMirrorEpochLookup(
            String mirrorName, Set<TopicPartition> topicPartitionSet, Collection<ClusterMirrorListing> sourceMirrors) {
        return sourceSyncer.sendLastMirrorEpochLookup(mirrorName, topicPartitionSet, sourceMirrors);
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
    Map<String, Map<TopicPartition, Integer>> processLastMirrorEpochLookup(
            Map<String, Map<String, Set<Integer>>> mirrorPartitions) {
        Map<String, Map<TopicPartition, Integer>> result = new HashMap<>();
        mirrorPartitions.forEach((mirrorName, topicParts) -> {
            topicParts.forEach((topic, parts) -> {
                parts.forEach(part -> {
                    ClusterMirrorRecordKey pk = ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topic), part);
                    PartitionCacheEntry cached = partitionCache.get(pk);
                    int lme = isLocalCoordinator(mirrorName, topic, part) && cached != null
                            ? cached.lastMirrorEpoch() : -1;
                    result.computeIfAbsent(mirrorName, k -> new HashMap<>())
                            .put(new TopicPartition(topic, part), lme);
                });
            });
        });
        return result;
    }

    /** Updates the partition cache with the given epochs, preserving existing state and failure info. */
    void updateLastMirrorEpochs(String mirrorName, Map<String, Map<Integer, Integer>> addedEpochs) {
        addedEpochs.forEach((topic, partitionEpochs) -> {
            partitionEpochs.forEach((partition, epoch) -> {
                ClusterMirrorRecordKey key = ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topic), partition);
                partitionCache.compute(key, (k, existing) -> {
                    MirrorPartitionState state = existing != null ? existing.state() : null;
                    FailedPartitionInfo fpi = existing != null ? existing.failedInfo() : null;
                    return new PartitionCacheEntry(state, epoch, fpi);
                });
            });
        });
    }
}
