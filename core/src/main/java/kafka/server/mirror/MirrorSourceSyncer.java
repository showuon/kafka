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
import kafka.server.mirror.MirrorStateCache.SourceLeader;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ClusterMirrorDescription;
import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsOptions;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListClusterMirrorsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.StartMirrorTopicsOptions;
import org.apache.kafka.clients.admin.StopMirrorTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BumpLeaderEpochsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DescribeClusterMirrorsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.BumpLeaderEpochsRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig;
import org.apache.kafka.coordinator.mirror.MirrorPartitionKey;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.MirrorUtils;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import com.google.re2j.Pattern;

import org.slf4j.Logger;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import scala.Option;

import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Periodically syncs source cluster state (topic metadata, configs, group offsets, ACLs)
 * on every broker, and runs coordinator-only operations (topic creation, partition scaling,
 * pattern discovery) on the broker that leads the mirror's {@code __mirror_state} partition.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
class MirrorSourceSyncer {
    static final int LEADER_EPOCH_BUMP_THRESHOLD = 3;
    static final int LEADER_EPOCH_BUMP_INCREMENT = 10;

    private final Logger log;
    private final KafkaConfig brokerConfig;
    private final int nodeId;

    private final MirrorMetadataManager metadataManager;
    private final NodeToControllerChannelManager channelManager;
    private final MirrorStateCache mirrorCache;
    private final MetadataCache metadataCache;
    private final KafkaScheduler scheduler;

    private volatile ScheduledFuture<?> metadataRefreshFuture;

    private final AtomicLong metadataRefreshError;
    private final AtomicLong topicConfigSyncError;
    private final AtomicLong consumerGroupOffsetSyncError;
    private final AtomicLong shareGroupOffsetSyncError;
    private final AtomicLong aclSyncError;

    MirrorSourceSyncer(
        KafkaConfig brokerConfig,
        MirrorMetadataManager metadataManager,
        NodeToControllerChannelManager channelManager,
        MetadataCache metadataCache,
        MirrorStateCache mirrorCache,
        KafkaScheduler scheduler,
        AtomicLong metadataRefreshError,
        AtomicLong topicConfigSyncError,
        AtomicLong consumerGroupOffsetSyncError,
        AtomicLong shareGroupOffsetSyncError,
        AtomicLong aclSyncError
    ) {
        this.brokerConfig = brokerConfig;
        this.nodeId = brokerConfig.nodeId();
        String name = "[" + MirrorSourceSyncer.class.getSimpleName() + " id=" + nodeId + "] ";
        this.log = new LogContext(name).logger(MirrorSourceSyncer.class);

        this.metadataManager = metadataManager;
        this.channelManager = channelManager;
        this.mirrorCache = mirrorCache;
        this.metadataCache = metadataCache;
        this.scheduler = scheduler;

        this.metadataRefreshError = metadataRefreshError;
        this.topicConfigSyncError = topicConfigSyncError;
        this.consumerGroupOffsetSyncError = consumerGroupOffsetSyncError;
        this.shareGroupOffsetSyncError = shareGroupOffsetSyncError;
        this.aclSyncError = aclSyncError;
    }

    /**
     * Checks whether this broker leads the __mirror_state partition for the given mirror name.
     * Hashes by mirror name only, so all mirror-level work (source sync, config sync) is handled
     * by a single broker per mirror.
     */
    boolean isLocalCoordinator(String mirrorName) {
        MetadataImage image = metadataManager.metadataImage();
        if (image.topics().getTopic(MIRROR_STATE_TOPIC_NAME) != null) {
            int partition = Utils.abs(mirrorName.hashCode())
                % brokerConfig.mirrorConfig().stateTopicNumPartitions();
            int leader = image.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(partition).leader;
            return leader == nodeId;
        }
        return false;
    }

    /**
     * Schedules (or reschedules) the periodic source sync at the given interval.
     * Each tick validates the source cluster ID, then on the coordinator broker
     * syncs topic state, configs, group offsets, ACLs, and topic patterns.
     */
    void scheduleMetadataRefresh(long intervalMs) {
        ScheduledFuture<?> oldFuture = metadataRefreshFuture;
        if (oldFuture != null) {
            oldFuture.cancel(false);
        }
        metadataRefreshFuture = scheduler.schedule("MirrorMetadataRefresh",
                this::runMetadataRefresh, intervalMs, intervalMs);
        log.info("Scheduled metadata refresh with interval {} ms", intervalMs);
    }

    /**
     * Periodic source sync callback. Every broker syncs topic state from the source
     * (needed for source leader caches, deletion detection, and missed partition recovery).
     * Only the coordinator syncs configs, group offsets, and ACLs.
     */
    private void runMetadataRefresh() {
        retryPendingTombstoneWrites();

        Set<String> mirrors = metadataManager.getConfiguredMirrors();
        if (mirrors.isEmpty()) {
            return;
        }

        log.info("Refreshing metadata for mirrors: {}", mirrors);

        for (String mirrorName : mirrors) {
            try {
                validateSourceClusterId(mirrorName);
                var topicState = syncSourceTopicState(mirrorName);
                syncSourceConfigsAndOffsets(mirrorName, topicState);
            } catch (Exception e) {
                log.error("Failed to refresh metadata for mirror {}", mirrorName, e);
            }
        }

        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        metadataRefreshError.incrementAndGet();
    }

    private void retryPendingTombstoneWrites() {
        Set<String> configuredMirrors = metadataManager.getConfiguredMirrors();
        Set<String> staleMirrors = mirrorCache.partitionKeys().stream()
                .map(MirrorPartitionKey::mirrorName)
                .filter(name -> !configuredMirrors.contains(name))
                .collect(Collectors.toSet());
        for (String mirrorName : staleMirrors) {
            log.info("Found stale partition states for deleted mirror '{}'. Writing tombstones.", mirrorName);
            metadataManager.tombstoneMirror(mirrorName);
        }
    }

    private void validateSourceClusterId(String mirrorName) {
        Admin srcAdmin = metadataManager.getOrCreateSourceAdmin(mirrorName);
        try {
            var clusterResult = srcAdmin.describeCluster();
            String newClusterId = clusterResult.clusterId().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            if (newClusterId != null && !newClusterId.isEmpty()) {
                String previousClusterId = metadataManager.getSourceClusterId(mirrorName);
                if (previousClusterId != null && !previousClusterId.equals(newClusterId)) {
                    String errMsg = "Source cluster ID changed for mirror " + mirrorName
                            + ": expected " + previousClusterId + ", got " + newClusterId
                            + ". This may indicate a misconfiguration or that the source cluster has been replaced. "
                            + "Moving all partitions to non-retryable failed state.";
                    log.error(errMsg);

                    Set<String> mirroredTopics = metadataManager.getConfiguredTopics(mirrorName, true);
                    if (!mirroredTopics.isEmpty()) {
                        Set<TopicPartition> mirroredLeaderPartitions = new HashSet<>();
                        for (String topic : mirroredTopics) {
                            TopicImage topicImage = metadataManager.metadataImage().topics().getTopic(topic);
                            if (topicImage != null) {
                                topicImage.partitions().forEach((partitionId, partition) -> {
                                    if (partition.leader == nodeId) {
                                        mirroredLeaderPartitions.add(new TopicPartition(topic, partitionId));
                                    }
                                });
                            }
                        }
                        if (!mirroredLeaderPartitions.isEmpty()) {
                            metadataManager.transitionTo(mirrorName, mirroredLeaderPartitions, MirrorPartitionState.FAILED, errMsg, true);
                        }
                    }
                }
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Failed to describe source cluster for mirror {}", mirrorName, e);
        }
    }

    /**
     * Lists the cluster mirrors configured on the source cluster, including their active topic names.
     * Used before mirroring to detect mirror loops and during truncation to validate that source
     * partitions are stopped.
     *
     * @param mirrorName the name of the local mirror whose source cluster to query
     * @return the cluster mirror listings from the source cluster, or an empty list if the source
     *         cluster does not support the ListClusterMirrors API
     * @throws IllegalStateException if the request fails
     */
    Collection<ClusterMirrorListing> listSourceClusterMirrors(String mirrorName) {
        Admin srcAdmin = metadataManager.getOrCreateSourceAdmin(mirrorName);
        try {
            return srcAdmin
                    .listClusterMirrors(new ListClusterMirrorsOptions().shouldIncludeTopicNames(true))
                    .all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnsupportedVersionException) {
                log.info("Source cluster does not support listClusterMirrors for mirror {}. Skipping mirror loop check.", mirrorName);
                return List.of();
            }
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalStateException("Failed to list cluster mirrors from source for mirror "
                    + mirrorName + ": " + cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Failed to list cluster mirrors from source for mirror "
                    + mirrorName + ": " + e.getMessage(), e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to list cluster mirrors from source for mirror "
                    + mirrorName + ": " + e.getMessage(), e);
        }
    }

    /**
     * Checks whether mirroring the given partitions from the source cluster would create
     * a mirror loop, based on the mirrors currently configured on the source cluster.
     *
     * @param mirrorName      the local mirror being started
     * @param topicPartitions the partitions about to be mirrored
     * @param sourceMirrors   mirrors configured on the source cluster, as returned by
     *                        {@link #listSourceClusterMirrors(String)}
     * @return true if the given partitions would create a mirror loop, false otherwise
     */
    boolean hasMirrorLoop(String mirrorName, TopicPartition tp,
                          Collection<ClusterMirrorListing> sourceMirrors) {
        if (sourceMirrors.isEmpty()) {
            return false;
        }
        String topicName = tp.topic();
        for (ClusterMirrorListing sourceMirror : sourceMirrors) {
            if (!metadataManager.clusterId().equals(sourceMirror.sourceClusterId())) {
                continue;
            }
            if (sourceMirror.topics().contains(topicName)) {
                log.error("Mirror loop detected for mirror {}: source mirror {} is already mirroring topic {}",
                        mirrorName, sourceMirror.mirrorName(), topicName);
                return true;
            }
        }
        return false;
    }

    /** Schedules an immediate one-shot source topic state sync for the given mirror. */
    void scheduleSourceTopicStateSync(String mirrorName) {
        scheduler.scheduleOnce("source-topic-state-sync", () -> syncSourceTopicState(mirrorName));
    }

    /**
     * Fetches topics metadata from the source cluster via Admin.describeTopics.
     * Runs on every broker to keep partition leaders, topic creation, topic deletion, and partition counts in sync.
     */
    Optional<List<SourceTopicState>> syncSourceTopicState(String mirrorName) {
        log.info("Syncing source topic state for mirror {}", mirrorName);
        Set<String> topics = metadataManager.getConfiguredTopics(mirrorName, false);
        if (topics.isEmpty()) {
            return Optional.empty();
        }

        Admin srcAdmin = metadataManager.getOrCreateSourceAdmin(mirrorName);

        try {
            Map<String, KafkaFuture<TopicDescription>> futures = srcAdmin.describeTopics(topics).topicNameValues();
            List<SourceTopicState> result = new ArrayList<>();

            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                try {
                    TopicDescription td = entry.getValue().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                    List<SourcePartitionState> partitions = td.partitions().stream()
                            .map(pi -> new SourcePartitionState(
                                    new TopicPartition(td.name(), pi.partition()),
                                    pi.leader(),
                                    pi.leaderEpoch()))
                            .toList();
                    result.add(new SourceTopicState(td.name(), td.topicId(), true, partitions));
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        result.add(new SourceTopicState(entry.getKey(), Uuid.ZERO_UUID, false, List.of()));
                    } else {
                        log.warn("Failed to describe topic {} for mirror {}", entry.getKey(), mirrorName, e.getCause());
                    }
                }
            }

            processSourceTopicState(mirrorName, result);
            return Optional.of(result);
        } catch (Exception e) {
            log.warn("Failed to sync source topic state for mirror {}", mirrorName, e);
            return Optional.empty();
        }
    }

    private void processSourceTopicState(String mirrorName, List<SourceTopicState> sourceTopicStates) {
        var creatableTopics = new ArrayList<CreateTopicsRequestData.CreatableTopic>();
        var createPartitionsTopics = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();

        sourceTopicStates.forEach(ti -> {
            int sourcePartitionCount = ti.partitions().size();

            ti.partitions().forEach(pi -> {
                if (pi.leader() != null) {
                    mirrorCache.updateSourceLeader(mirrorName, pi.topicPartition(),
                            new SourceLeader(pi.leader(), pi.leaderEpoch().orElse(0)));
                }
            });

            // Pre-KIP-516 sources (Kafka < 2.8) return ZERO_UUID; fall back to name-based lookup
            TopicImage destTopic = !ti.topicId().equals(Uuid.ZERO_UUID)
                    ? metadataManager.metadataImage().topics().getTopic(ti.topicId())
                    : metadataManager.metadataImage().topics().getTopic(ti.topic());

            if (destTopic != null && destTopic.partitions().size() < sourcePartitionCount) {
                createPartitionsTopics.add(new CreatePartitionsRequestData.CreatePartitionsTopic()
                        .setName(ti.topic())
                        .setCount(sourcePartitionCount)
                        .setAssignments(null)
                );
            } else if (destTopic == null &&
                    metadataManager.metadataImage().topics().getTopic(ti.topic()) == null &&
                    ti.exists() && sourcePartitionCount > 0) {
                if (mirrorCache.addPendingTopicCreation(ti.topic())) {
                    creatableTopics.add(new CreateTopicsRequestData.CreatableTopic()
                            .setName(ti.topic())
                            .setNumPartitions(sourcePartitionCount)
                            .setReplicationFactor(CreateTopicsRequest.NO_REPLICATION_FACTOR)
                            .setMirrorInfo(new CreateTopicsRequestData.MirrorInfo().setTopicId(
                                    ti.topicId().equals(Uuid.ZERO_UUID) ? Uuid.randomUuid() : ti.topicId())));
                }
            } else if (destTopic == null &&
                    metadataManager.metadataImage().topics().getTopic(ti.topic()) != null &&
                    ti.exists()) {
                log.error("Mirror topic {} exists on destination with TopicId {} but source has TopicId {}. "
                                + "Delete the topic on destination and let auto-creation recreate it with the correct TopicId.",
                        ti.topic(), metadataManager.metadataImage().topics().getTopic(ti.topic()).id(), ti.topicId());
            }
        });

        if (isLocalCoordinator(mirrorName)) {
            if (!creatableTopics.isEmpty()) {
                createMirrorTopics(creatableTopics);
            }
            maybeScalePartitions(createPartitionsTopics);
        }

        maybeFailDeletedTopics(mirrorName, sourceTopicStates);
        maybeStartMissedPartitions(mirrorName);
    }

    /**
     * Creates mirror topics on the destination with the source's TopicId,
     * preserving topic identity across clusters. Called during periodic metadata
     * sync when topics have mirror.name config but don't exist on the destination yet.
     * Once created, onMetadataUpdate will detect them and start the mirror state machine.
     */
    private void createMirrorTopics(List<CreateTopicsRequestData.CreatableTopic> creatableTopics) {
        var topicNames = creatableTopics.stream().map(CreateTopicsRequestData.CreatableTopic::name).toList();
        creatableTopics.forEach(t -> log.info("Creating mirror topic {} on destination (partitions={}, topicId={})",
                t.name(), t.numPartitions(), t.mirrorInfo().topicId()));
        var createTopicsData = new CreateTopicsRequestData().setTimeoutMs(brokerConfig.requestTimeoutMs());
        creatableTopics.forEach(createTopicsData.topics()::add);
        ControllerRequestCompletionHandler requestCompletionHandler = new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                topicNames.forEach(mirrorCache::removePendingTopicCreation);
                log.warn("Create mirror topics timed out for {}", topicNames);
            }

            @Override
            public void onComplete(ClientResponse response) {
                topicNames.forEach(mirrorCache::removePendingTopicCreation);
                if (response.responseBody() instanceof CreateTopicsResponse createTopicsResponse) {
                    createTopicsResponse.data().topics().forEach(topic -> {
                        var error = Errors.forCode(topic.errorCode());
                        if (error != Errors.NONE) {
                            log.warn("Failed to create mirror topic {}: {}", topic.name(), error.message());
                        }
                    });
                }
            }
        };
        channelManager.sendRequest(
                new CreateTopicsRequest.Builder(createTopicsData),
                requestCompletionHandler);
    }

    private void maybeScalePartitions(CreatePartitionsRequestData.CreatePartitionsTopicCollection topics) {
        if (!topics.isEmpty()) {
            log.debug("Detected partition count change, sending CreatePartitionsRequest: {}", topics);
            channelManager.sendRequest(new CreatePartitionsRequest.Builder(
                    new CreatePartitionsRequestData()
                            .setTopics(topics)
                            .setValidateOnly(false)
                            .setTimeoutMs(3000)
            ), new TimeoutHandler(log));
        }
    }

    private void maybeFailDeletedTopics(String mirrorName, List<SourceTopicState> sourceTopicStates) {
        List<String> deletedSourceTopicNames = new ArrayList<>(sourceTopicStates.stream()
                .filter(ti -> !ti.exists())
                .map(SourceTopicState::topic).toList());

        if (deletedSourceTopicNames.isEmpty()) {
            return;
        }

        // In old cluster, it is possible the broker metadata update in progress, and the returned metadata response is stale.
        // list topic again to make sure it is indeed deleted.
        Admin srcAdmin = metadataManager.getOrCreateSourceAdmin(mirrorName);
        try {
            Set<String> allTopics = srcAdmin.listTopics().names().get();
            log.debug("Source topic name list: {}", allTopics);
            deletedSourceTopicNames.removeAll(allTopics);
        } catch (Exception e) {
            log.warn("Failed to list topics for mirror {}, skipping deleted topic detection: {}", mirrorName, e.getMessage());
            return;
        }

        metadataManager.getConfiguredTopics(mirrorName, true).forEach(name -> {
            if (deletedSourceTopicNames.contains(name)) {
                log.info("Detected topic {} deleted in remote cluster {}, marking mirror partitions as non-retryable", name, mirrorName);
                TopicImage topicImage = metadataManager.metadataImage().topics().getTopic(name);
                if (topicImage != null) {
                    topicImage.partitions().forEach((partitionId, partition) ->
                            metadataManager.transitionTo(mirrorName, Set.of(new TopicPartition(name, partitionId)),
                                    MirrorPartitionState.FAILED, "The source topic is deleted.", true));
                }
            }
        });
    }

    /**
     * Transitions partitions stuck in UNKNOWN because their source leader was not yet known
     * when onMetadataUpdate first ran.
     * <p>
     * The race: processSourceTopicState creates a mirror topic on the destination, which
     * triggers onMetadataUpdate. That callback needs the source leader in sourceLeaders to
     * transition the partition to LOG_TRUNCATION. If the source has not elected a leader yet
     * (or the Admin describeTopics response arrived without one), the partition stays in
     * UNKNOWN. Since onMetadataUpdate only fires on destination metadata changes, it will
     * not retry on its own.
     * <p>
     * This is more likely against older source clusters (e.g. Kafka 2.1 with ZK) where
     * Admin.describeTopics goes through three round trips before returning topic metadata:
     * describeCluster (node discovery), DescribeTopicPartitions (rejected with
     * UnsupportedVersionException), then MetadataRequest (fallback). The extra latency
     * widens the window in which the source leader is not yet known.
     */
    private void maybeStartMissedPartitions(String mirrorName) {
        var partitionLeaders = mirrorCache.getSourceLeaders(mirrorName);
        if (partitionLeaders == null) {
            return;
        }
        partitionLeaders.keySet().forEach(tp -> {
            var key = MirrorPartitionKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
            var cachedEntry = mirrorCache.getPartition(key);
            if (cachedEntry != null && cachedEntry.state() != null && cachedEntry.state() != MirrorPartitionState.UNKNOWN) {
                return;
            }
            TopicImage topicImage = metadataManager.metadataImage().topics().getTopic(tp.topic());
            if (topicImage == null) {
                return;
            }
            byte desiredState = topicImage.desiredMirrorState();
            if (desiredState == MirrorPartitionState.STOPPED.value()
                    || desiredState == MirrorPartitionState.PAUSED.value()) {
                return;
            }
            var partition = topicImage.partitions().get(tp.partition());
            if (partition != null && partition.leader == nodeId) {
                log.info("Source leader for {} discovered after initial onMetadataUpdate, transitioning to LOG_TRUNCATION", tp);
                metadataManager.transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.LOG_TRUNCATION);
            }
        });
    }

    /**
     * Syncs topic configurations, consumer/share group offsets, ACLs, and topic patterns
     * from the source cluster. Runs only on the coordinator broker for each mirror.
     */
    private void syncSourceConfigsAndOffsets(String mirrorName, Optional<List<SourceTopicState>> sourceTopicStates) {
        if (!isLocalCoordinator(mirrorName)) {
            return;
        }

        log.info("Syncing source configs and offsets for mirror {}", mirrorName);

        try {
            ClusterMirrorConfig mirrorConfig = ClusterMirrorConfig.fromProperties(
                    metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName)));
            syncTopicConfigs(mirrorName, mirrorConfig);
            syncGroupOffsets(mirrorName, mirrorConfig);
            syncAcls(mirrorName, mirrorConfig);
            if (!metadataManager.getConfiguredTopics(mirrorName, false, false).isEmpty()) {
                maybeBumpLeaderEpochs(mirrorName, sourceTopicStates, Set.of());
            }
            discoverTopicsByPattern(mirrorName, mirrorConfig);
            enforceExcludePatterns(mirrorName, mirrorConfig);
        } catch (Exception e) {
            log.error("Failed to sync mirror metadata for mirror {}", mirrorName, e);
        }
    }

    private void syncTopicConfigs(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        Admin srcAdmin = metadataManager.getOrCreateSourceAdmin(mirrorName);

        Set<String> topics = metadataManager.getConfiguredTopics(mirrorName, false);
        log.debug("Describing topic configs for topics: {}", topics);
        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        topicConfigSyncError.incrementAndGet();

        Collection<ConfigResource> resources = topics.stream()
                .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                .toList();

        try {
            Map<ConfigResource, Config> sourceConfigs = srcAdmin.describeConfigs(resources)
                    .all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            Map<String, Map<String, String>> configsToChange = detectConfigurationChanges(sourceConfigs, mirrorConfig);
            applyConfigurationChanges(configsToChange);
        } catch (Exception e) {
            log.warn("Failed to describe topic configs for mirror {}: {}", mirrorName, e.getMessage());
        }
    }

    private Map<String, Map<String, String>> detectConfigurationChanges(
            Map<ConfigResource, Config> sourceConfigs, ClusterMirrorConfig mirrorConfig) {
        Map<String, Map<String, String>> configsToChange = new HashMap<>();
        Pattern excludePattern = mirrorConfig.topicPropertiesExcludePattern();

        sourceConfigs.forEach((resource, config) -> {
            if (resource.type() == ConfigResource.Type.TOPIC) {
                Properties props = metadataCache.topicConfig(resource.name());
                Map<String, String> conChange = new HashMap<>();

                config.entries().forEach(entry -> {
                    if (entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG
                            && (excludePattern == null || !excludePattern.matcher(entry.name()).matches())) {
                        if (props.containsKey(entry.name())) {
                            if (!props.get(entry.name()).equals(entry.value())) {
                                conChange.put(entry.name(), entry.value());
                            }
                        } else {
                            conChange.put(entry.name(), entry.value());
                        }
                    }
                });

                if (!conChange.isEmpty()) {
                    configsToChange.put(resource.name(), conChange);
                }
            }
        });

        return configsToChange;
    }

    private void applyConfigurationChanges(Map<String, Map<String, String>> configsToChange) {
        log.debug("Applying config change: {}", configsToChange);

        Map<ConfigResource, Collection<AlterConfigOp>> configOps = new HashMap<>();
        configsToChange.forEach((name, changes) -> {
            var changeList = changes.entrySet().stream()
                    .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET))
                    .toList();
            configOps.put(new ConfigResource(ConfigResource.Type.TOPIC, name), changeList);
        });

        if (!configOps.isEmpty()) {
            IncrementalAlterConfigsRequestData data = new IncrementalAlterConfigsRequestData().setValidateOnly(false);
            for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : configOps.entrySet()) {
                ConfigResource resource = entry.getKey();
                IncrementalAlterConfigsRequestData.AlterableConfigCollection alterableConfigSet =
                        new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
                for (AlterConfigOp configEntry : configOps.get(resource))
                    alterableConfigSet.add(new IncrementalAlterConfigsRequestData.AlterableConfig()
                            .setName(configEntry.configEntry().name())
                            .setValue(configEntry.configEntry().value())
                            .setConfigOperation(configEntry.opType().id()));
                IncrementalAlterConfigsRequestData.AlterConfigsResource alterConfigsResource =
                        new IncrementalAlterConfigsRequestData.AlterConfigsResource();
                alterConfigsResource.setResourceType(resource.type().id())
                        .setResourceName(resource.name()).setConfigs(alterableConfigSet);
                data.resources().add(alterConfigsResource);
            }
            channelManager.sendRequest(new IncrementalAlterConfigsRequest.Builder(data), new TimeoutHandler(log));
        }
    }

    /**
     * Syncs group offsets from the source cluster to the destination in two phases:
     * consumer groups first, then share groups. Keeping them separate avoids cross-type
     * conflicts where a consumer group on the source could overwrite a share group with
     * the same name on the destination (or vice versa).
     */
    private void syncGroupOffsets(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        Admin srcAdmin = metadataManager.getOrCreateSourceAdmin(mirrorName);

        Set<String> mirrorTopics = metadataManager.getConfiguredTopics(mirrorName, false, false);
        if (mirrorTopics.isEmpty()) {
            return;
        }

        Pattern groupsIncludePattern = mirrorConfig.groupsIncludePattern();
        Pattern groupsExcludePattern = mirrorConfig.groupsExcludePattern();

        syncConsumerGroupOffsets(srcAdmin, mirrorName, mirrorTopics, groupsIncludePattern, groupsExcludePattern);
        syncShareGroupOffsets(srcAdmin, mirrorName, mirrorTopics, groupsIncludePattern, groupsExcludePattern);
    }

    private void syncConsumerGroupOffsets(Admin srcAdmin, String mirrorName, Set<String> mirrorTopics,
                                          Pattern groupsIncludePattern, Pattern groupsExcludePattern) {
        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        consumerGroupOffsetSyncError.incrementAndGet();
        try {
            List<String> sourceGroupIds = listSourceGroupIds(srcAdmin, ListGroupsOptions.forConsumerGroups(),
                    groupsIncludePattern, groupsExcludePattern);
            if (sourceGroupIds.isEmpty()) {
                return;
            }
            log.info("Syncing consumer group offsets for mirror {}, groups={}", mirrorName, sourceGroupIds);

            Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = sourceGroupIds.stream()
                    .collect(Collectors.toMap(id -> id, id -> new ListConsumerGroupOffsetsSpec()));
            Map<String, Map<TopicPartition, OffsetAndMetadata>> allOffsets = srcAdmin
                    .listConsumerGroupOffsets(groupSpecs).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            Optional<Set<String>> activeDestGroups = getActiveDestinationGroupIds(ListGroupsOptions.forConsumerGroups());
            if (activeDestGroups.isEmpty()) {
                return;
            }

            for (var entry : allOffsets.entrySet()) {
                String groupId = entry.getKey();
                if (activeDestGroups.get().contains(groupId)) {
                    log.warn("Skipping consumer group offset sync for group {} in mirror {}: active on destination", groupId, mirrorName);
                    continue;
                }

                Map<TopicPartition, OffsetAndMetadata> filtered = new HashMap<>();
                entry.getValue().entrySet().stream()
                        .filter(e -> mirrorTopics.contains(e.getKey().topic()))
                        .forEach(ent -> {
                            TopicPartition topicPartition = ent.getKey();
                            Option<Long> logStartOffset = metadataManager.replicaManagerSupplier().get().getLog(topicPartition).map(UnifiedLog::logStartOffset);
                            Option<Long> logEndOffset = metadataManager.replicaManagerSupplier().get().getLog(topicPartition).map(UnifiedLog::logEndOffset);
                            if (logStartOffset.isEmpty() ||  logEndOffset.isEmpty()) {
                                log.debug("Cannot get the log start offset or log end offset for partition {}, skip consumer group sync for it.", topicPartition);
                                return;
                            }
                            OffsetAndMetadata sourceGroupOffsetAndMetadata = ent.getValue();

                            // Committing to the range [local logStartOffset ~ local logEndOffset]
                            long finalOffset = Math.max(logStartOffset.get(), Math.min(sourceGroupOffsetAndMetadata.offset(), logEndOffset.get()));

                            if (finalOffset == sourceGroupOffsetAndMetadata.offset()) {
                                filtered.put(topicPartition, sourceGroupOffsetAndMetadata);
                            } else if (finalOffset == logEndOffset.get()) {
                                int logEndEpoch = metadataManager.replicaManagerSupplier().get().getLog(topicPartition).map(l -> l.leaderEpochCache().epochForOffset(logEndOffset.get()).orElse(-1)).getOrElse(() -> -1);
                                if (logEndEpoch < 0) {
                                    log.debug("Cannot get the log end epoch for partition {}, skip consumer group sync for it.", topicPartition);
                                } else {
                                    filtered.put(topicPartition, new OffsetAndMetadata(logEndOffset.get(), Optional.of(logEndEpoch), ""));
                                }
                            } else {
                                // finalOffset == logStartOffset
                                int logStartEpoch = metadataManager.replicaManagerSupplier().get().getLog(topicPartition).map(l -> l.leaderEpochCache().epochForOffset(logStartOffset.get()).orElse(-1)).getOrElse(() -> -1);
                                if (logStartEpoch < 0) {
                                    log.debug("Cannot get the log start epoch for partition {}, skip consumer group sync for it.", topicPartition);
                                } else {
                                    filtered.put(topicPartition, new OffsetAndMetadata(logStartOffset.get(), Optional.of(logStartEpoch), ""));
                                }
                            }
                        });

                if (filtered.isEmpty()) {
                    continue;
                }

                try {
                    log.debug("Committing consumer group offsets for group {} on destination, partitions={}", groupId, filtered.keySet());
                    metadataManager.getOrCreateDestAdmin().alterConsumerGroupOffsets(groupId, filtered).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    log.warn("Failed to commit consumer group offsets for group {} in mirror {}: {}", groupId, mirrorName, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to sync consumer group offsets for mirror {}: {}", mirrorName, e.getMessage());
        }
    }

    private void syncShareGroupOffsets(Admin srcAdmin, String mirrorName, Set<String> mirrorTopics,
                                       Pattern groupsIncludePattern, Pattern groupsExcludePattern) {
        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        shareGroupOffsetSyncError.incrementAndGet();
        try {
            List<String> sourceGroupIds;
            try {
                sourceGroupIds = listSourceGroupIds(srcAdmin, ListGroupsOptions.forShareGroups(),
                        groupsIncludePattern, groupsExcludePattern);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnsupportedVersionException) {
                    log.debug("The source cluster doesn't support share group, skipping share group offset sync");
                    return;
                } else {
                    throw e;
                }
            }
            if (sourceGroupIds.isEmpty()) {
                return;
            }
            log.info("Syncing share group offsets for mirror {}, groups={}", mirrorName, sourceGroupIds);

            Map<String, ListShareGroupOffsetsSpec> groupSpecs = sourceGroupIds.stream()
                    .collect(Collectors.toMap(id -> id, id -> new ListShareGroupOffsetsSpec()));
            Map<String, Map<TopicPartition, OffsetAndMetadata>> allOffsets = srcAdmin
                    .listShareGroupOffsets(groupSpecs).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            Optional<Set<String>> activeDestGroups = getActiveDestinationGroupIds(ListGroupsOptions.forShareGroups());
            if (activeDestGroups.isEmpty()) {
                return;
            }

            for (var entry : allOffsets.entrySet()) {
                String groupId = entry.getKey();
                if (activeDestGroups.get().contains(groupId)) {
                    log.warn("Skipping share group offset sync for group {} in mirror {}: active on destination", groupId, mirrorName);
                    continue;
                }

                Map<TopicPartition, Long> filtered = new  HashMap<>();
                entry.getValue().entrySet().stream()
                        .filter(e -> mirrorTopics.contains(e.getKey().topic()))
                        .forEach(ent -> {
                            TopicPartition topicPartition = ent.getKey();
                            Option<Long> logStartOffset = metadataManager.replicaManagerSupplier().get().getLog(topicPartition).map(UnifiedLog::logStartOffset);
                            Option<Long> logEndOffset = metadataManager.replicaManagerSupplier().get().getLog(topicPartition).map(UnifiedLog::logEndOffset);
                            if (logStartOffset.isEmpty() ||  logEndOffset.isEmpty()) {
                                log.debug("Cannot get the log start offset or log end offset for partition {}, skip share group offset sync for it.", topicPartition);
                                return;
                            }
                            OffsetAndMetadata sourceGroupOffsetAndMetadata = ent.getValue();
                            // Committing to the range [local logStartOffset ~ local logEndOffset]
                            long finalOffset = Math.max(logStartOffset.get(), Math.min(sourceGroupOffsetAndMetadata.offset(), logEndOffset.get()));
                            filtered.put(topicPartition, finalOffset);
                        });
                if (filtered.isEmpty()) {
                    continue;
                }

                try {
                    log.debug("Committing share group offsets for group {} on destination, partitions={}", groupId, filtered.keySet());
                    metadataManager.getOrCreateDestAdmin().alterShareGroupOffsets(groupId, filtered).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    log.warn("Failed to commit share group offsets for group {} in mirror {}: {}", groupId, mirrorName, e);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to sync share group offsets for mirror {}: {}", mirrorName, e);
        }
    }

    private List<String> listSourceGroupIds(Admin srcAdmin, ListGroupsOptions options,
                                            Pattern groupsIncludePattern, Pattern groupsExcludePattern) throws Exception {
        return srcAdmin.listGroups(options).all()
                .get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS).stream()
                .map(GroupListing::groupId)
                .filter(id -> groupsIncludePattern == null || groupsIncludePattern.matcher(id).matches())
                .filter(id -> groupsExcludePattern == null || !groupsExcludePattern.matcher(id).matches())
                .toList();
    }

    /**
     * Returns the set of destination group IDs that should not be overwritten during offset sync
     * (groups in STABLE, PREPARING_REBALANCE, COMPLETING_REBALANCE, ASSIGNING, or RECONCILING state).
     *
     * @return the group IDs to skip, or empty Optional on failure so the caller can skip the sync cycle
     */
    private Optional<Set<String>> getActiveDestinationGroupIds(ListGroupsOptions typeFilter) {
        try {
            var options = typeFilter.inGroupStates(Set.of(
                    GroupState.STABLE,
                    GroupState.PREPARING_REBALANCE,
                    GroupState.COMPLETING_REBALANCE,
                    GroupState.ASSIGNING,
                    GroupState.RECONCILING));
            return Optional.of(metadataManager.getOrCreateDestAdmin().listGroups(options).all()
                    .get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS).stream()
                    .map(GroupListing::groupId)
                    .collect(Collectors.toSet()));
        } catch (Exception e) {
            log.warn("Failed to list destination groups, skipping offset sync cycle.", e);
            return Optional.empty();
        }
    }

    private void syncAcls(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        // TODO: We currently mirror all ACLs from the source to the target.
        //       Any ACLs added/removed directly on the target will be overwritten
        //       on the next sync to match the source.
        //
        // TODO: How do we disambiguate ACLs that reference the same resource name
        //       when multiple cluster mirrors exist?

        Admin srcAdmin = metadataManager.getOrCreateSourceAdmin(mirrorName);

        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        aclSyncError.incrementAndGet();

        try {
            Collection<AclBinding> sourceAcls = srcAdmin.describeAcls(AclBindingFilter.ANY)
                    .values().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            log.debug("Describe ACLs response from remote cluster {}: {}", mirrorName, sourceAcls);

            List<MirrorUtils.AclRule> aclIncludeRules = mirrorConfig.aclIncludeRules();
            var allRemoteAcls = sourceAcls.stream()
                    .filter(acl -> aclIncludeRules.stream().anyMatch(rule -> rule.matches(acl)))
                    .toList();
            var aclChanges = detectAclChanges(allRemoteAcls);
            applyAclChanges(mirrorName, aclChanges);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SecurityDisabledException) {
                log.debug("ACL sync skipped for mirror {}: {}", mirrorName, e.getCause().getMessage());
            } else {
                log.warn("Failed to describe ACLs for mirror {}: {}", mirrorName, e.getMessage());
            }
        } catch (Exception e) {
            log.warn("Failed to sync ACLs for mirror {}: {}", mirrorName, e.getMessage());
        }
    }

    private SourceAclChanges detectAclChanges(List<AclBinding> sourceAcls) {
        var addACLsList = new ArrayList<AclBinding>();
        var deleteACLsList = new ArrayList<AclBinding>();
        var current = metadataManager.metadataImage().acls().acls().values();

        sourceAcls.forEach(acl -> {
            if (current.stream().map(StandardAcl::toBinding).noneMatch(a -> a.equals(acl))) {
                addACLsList.add(acl);
            }
        });

        metadataManager.metadataImage().acls().acls().values().forEach(acl -> {
            if (acl.resourceType() != ResourceType.CLUSTER_MIRROR && !sourceAcls.contains(acl.toBinding())) {
                deleteACLsList.add(acl.toBinding());
            }
        });

        return new SourceAclChanges(addACLsList, deleteACLsList);
    }

    private void applyAclChanges(String mirrorName, SourceAclChanges aclChanges) {
        if (!aclChanges.aclsToAdd().isEmpty()) {
            log.debug("Adding {} ACLs from remote cluster {}", aclChanges.aclsToAdd().size(), mirrorName);
            var requestData = aclChanges.aclsToAdd().stream().map(
                            aclBinding -> new CreateAclsRequestData.AclCreation()
                                    .setResourceType(aclBinding.pattern().resourceType().code())
                                    .setResourceName(aclBinding.pattern().name())
                                    .setResourcePatternType(aclBinding.pattern().patternType().code())
                                    .setPrincipal(aclBinding.entry().principal())
                                    .setHost(aclBinding.entry().host())
                                    .setOperation(aclBinding.entry().operation().code())
                                    .setPermissionType(aclBinding.entry().permissionType().code()))
                    .toList();
            channelManager.sendRequest(
                    new CreateAclsRequest.Builder(new CreateAclsRequestData().setCreations(requestData)),
                    new TimeoutHandler(log)
            );
        }

        if (!aclChanges.aclsToDelete().isEmpty()) {
            log.debug("Removing {} ACLs from remote cluster {}", aclChanges.aclsToDelete().size(), mirrorName);
            var requestData = aclChanges.aclsToDelete().stream().map(
                            aclBinding -> new org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter()
                                    .setResourceTypeFilter(aclBinding.pattern().resourceType().code())
                                    .setResourceNameFilter(aclBinding.pattern().name())
                                    .setPatternTypeFilter(aclBinding.pattern().patternType().code())
                                    .setPrincipalFilter(aclBinding.entry().principal())
                                    .setHostFilter(aclBinding.entry().host())
                                    .setOperation(aclBinding.entry().operation().code())
                                    .setPermissionType(aclBinding.entry().permissionType().code()))
                    .toList();
            channelManager.sendRequest(
                    new DeleteAclsRequest.Builder(new DeleteAclsRequestData().setFilters(requestData)),
                    new TimeoutHandler(log)
            );
        }
    }

    private void discoverTopicsByPattern(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        final Pattern topicsIncludePattern = mirrorConfig.topicsIncludePattern();
        if (topicsIncludePattern == null) {
            return;
        }

        Admin srcAdmin = metadataManager.getOrCreateSourceAdmin(mirrorName);

        Set<String> configuredTopics = metadataManager.getConfiguredTopics(mirrorName, true);
        final Pattern topicsExcludePattern = mirrorConfig.topicsExcludePattern();

        List<StartMirrorTopicsRequestData.TopicMetadata> newTopics;
        try {
            Set<String> allSourceTopics = srcAdmin.listTopics()
                    .names().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            List<String> candidates = allSourceTopics.stream()
                    .filter(name -> topicsIncludePattern.matcher(name).matches())
                    .filter(name -> topicsExcludePattern == null || !topicsExcludePattern.matcher(name).matches())
                    .filter(name -> !configuredTopics.contains(name))
                    .toList();

            if (candidates.isEmpty()) {
                return;
            }

            Map<String, TopicDescription> descriptions = srcAdmin.describeTopics(candidates)
                    .allTopicNames().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            cacheSourceLeaders(mirrorName, descriptions.values());

            newTopics = descriptions.values().stream()
                    .map(td -> new StartMirrorTopicsRequestData.TopicMetadata()
                            .setTopicName(td.name())
                            .setTopicId(td.topicId())
                            .setNumPartitions(td.partitions().size()))
                    .toList();
        } catch (Exception e) {
            log.warn("Failed to discover topics by pattern for mirror {}", mirrorName, e);
            return;
        }

        if (newTopics.isEmpty()) {
            return;
        }

        log.info("Discovered {} new topic(s) matching mirror.topics.include pattern for mirror {}: {}",
                newTopics.size(), mirrorName, newTopics.stream().map(StartMirrorTopicsRequestData.TopicMetadata::topicName).toList());

        // TODO: creation failures from auto-discovery are silently lost here (fire-and-forget).
        //  Add per-topic status tracking so describeMirror can surface failed topics to users.
        try {
            metadataManager.getOrCreateDestAdmin().startMirrorTopics(
                    mirrorName,
                    newTopics.stream().map(StartMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet()),
                    new StartMirrorTopicsOptions()).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("Failed to start discovered topics for mirror {}: {}", mirrorName, e.getMessage());
        }
    }

    /**
     * Checks if any active mirror topics now match the exclude pattern and sends
     * StopMirrorTopicsRequest to stop them. Catches cases where exclude was updated
     * via incrementalAlterConfigs outside of the startMirrorTopics/stopMirrorTopics flow.
     */
    private void enforceExcludePatterns(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        Pattern excludePattern = mirrorConfig.topicsExcludePattern();
        if (excludePattern == null) return;

        Set<String> activeTopics = metadataManager.getConfiguredTopics(mirrorName, false, false);
        Set<String> excludedTopics = activeTopics.stream()
                .filter(topic -> excludePattern.matcher(topic).matches())
                .collect(Collectors.toSet());

        if (excludedTopics.isEmpty()) return;

        log.info("Stopping {} topic(s) matching mirror.topics.exclude for mirror {}: {}",
                excludedTopics.size(), mirrorName, excludedTopics);

        try {
            metadataManager.getOrCreateDestAdmin().stopMirrorTopics(mirrorName, excludedTopics, new StopMirrorTopicsOptions())
                    .all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("Failed to stop excluded topics for mirror {}: {}", mirrorName, e.getMessage());
        }
    }

    /** Schedules a source topic state sync followed by a leader epoch bump request. */
    CompletableFuture<Void> scheduleBumpLeaderEpoch(String mirrorName, TopicPartition tp) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        scheduler.scheduleOnce("bump-leader-epoch-" + tp, () -> {
            Optional<List<SourceTopicState>> sourceTopicStates = syncSourceTopicState(mirrorName);
            maybeBumpLeaderEpochs(mirrorName, sourceTopicStates, Set.of(tp))
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            future.completeExceptionally(ex);
                        } else {
                            future.complete(null);
                        }
                    });
        });
        return future;
    }

    private CompletableFuture<Void> maybeBumpLeaderEpochs(String mirrorName, Optional<List<SourceTopicState>> sourceTopicStates, Set<TopicPartition> topicPartitions) {
        return sourceTopicStates
                .map(topicStates -> sendBumpLeaderEpochs(buildSourceEpochBumpTargets(mirrorName, topicStates, topicPartitions))
                .whenComplete((v, ex) -> {
                    if (ex != null) log.warn("Failed to bump leader epoch for mirror {}", mirrorName, ex);
                })).orElseGet(() -> CompletableFuture.completedFuture(null));
    }

    /** Sends an AlterPartition request to bump leader epochs on the destination. */
    CompletableFuture<Void> sendBumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs) {
        if (partitionMinEpochs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        log.info("Sending bump leader epoch request: {}", partitionMinEpochs);
        CompletableFuture<Void> future = new CompletableFuture<>();

        List<BumpLeaderEpochsRequestData.TopicState> topicStates = new ArrayList<>();
        Map<String, Set<Integer>> partitions = new HashMap<>();
        partitionMinEpochs.keySet().forEach(tp -> {
            partitions.computeIfAbsent(tp.topic(), key -> new HashSet<>()).add(tp.partition());
        });
        partitions.forEach((topic, parts) -> {
            BumpLeaderEpochsRequestData.TopicState topicState = new BumpLeaderEpochsRequestData.TopicState();
            List<BumpLeaderEpochsRequestData.LeaderEpochState> topicLeaderEpoch = new ArrayList<>();
            parts.forEach(partitionId -> {
                TopicPartition tp = new TopicPartition(topic, partitionId);
                topicLeaderEpoch.add(new BumpLeaderEpochsRequestData.LeaderEpochState().setMinLeaderEpoch(partitionMinEpochs.get(tp)).setPartitionIndex(partitionId));
            });
            topicState.setTopicId(metadataCache.getTopicId(topic)).setPartitions(topicLeaderEpoch);
            topicStates.add(topicState);
        });

        mirrorCache.addPendingEpochBump(new MirrorStateCache.PendingLeaderEpochBump(future, new ConcurrentHashMap<>(partitionMinEpochs)));
        metadataManager.maybeCompletePendingEpochBumps();

        channelManager.sendRequest(new BumpLeaderEpochsRequest.Builder(
                new BumpLeaderEpochsRequestData().setTopics(topicStates)
        ), new ControllerRequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse response) {
                log.debug("Bump leader epoch response: {}", response);
            }

            @Override
            public void onTimeout() {
                log.warn("BumpLeaderEpoch request timed out");
            }
        });
        return future;
    }

    private Map<TopicPartition, Integer> buildSourceEpochBumpTargets(String mirrorName, List<SourceTopicState> sourceTopicStates, Set<TopicPartition> topicPartitions) {
        Set<String> mirrorTopics = topicPartitions.isEmpty() ? metadataManager.getConfiguredTopics(mirrorName, false) : Set.of();
        Map<TopicPartition, Integer> leaderEpochFromMetadata = new HashMap<>();
        for (SourceTopicState ts : sourceTopicStates) {
            if (!ts.exists()) {
                continue;
            }
            if (!mirrorTopics.isEmpty() && !mirrorTopics.contains(ts.topic())) {
                continue;
            }
            collectEpochBumpTargets(ts, topicPartitions, leaderEpochFromMetadata);
        }
        if (!leaderEpochFromMetadata.isEmpty()) {
            log.info("Bumping leader epoch for partitions {}", leaderEpochFromMetadata);
        }
        return leaderEpochFromMetadata;
    }

    private void collectEpochBumpTargets(SourceTopicState topicInfo,
                                         Set<TopicPartition> topicPartitions,
                                         Map<TopicPartition, Integer> leaderEpochFromMetadata) {
        for (SourcePartitionState ps : topicInfo.partitions()) {
            TopicPartition tp = ps.topicPartition();
            if (!topicPartitions.isEmpty() && !topicPartitions.contains(tp)) {
                continue;
            }
            if (ps.leaderEpoch().isEmpty()) {
                continue;
            }
            TopicImage topicImage = metadataManager.metadataImage().topics().getTopic(tp.topic());
            if (topicImage == null || topicImage.partitions().get(tp.partition()) == null) {
                continue;
            }
            int epoch = ps.leaderEpoch().get();
            int localEpoch = topicImage.partitions().get(tp.partition()).leaderEpoch;
            if (epoch > localEpoch - LEADER_EPOCH_BUMP_THRESHOLD) {
                int newEpoch = Math.addExact(epoch, LEADER_EPOCH_BUMP_INCREMENT);
                leaderEpochFromMetadata.put(tp, newEpoch);
            }
        }
    }

    /**
     * Pre-populates sourceLeaders for discovered topics so that when onMetadataUpdate fires
     * after the destination topic is created, the fetcher can connect to the correct source
     * broker immediately. Without this, the fetcher starts with a bootstrap broker, gets a
     * redirect it cannot resolve, and cycles through FAILED/retry until the next periodic
     * syncSourceTopicState populates the cache.
     */
    private void cacheSourceLeaders(String mirrorName, Collection<TopicDescription> descriptions) {
        descriptions.forEach(td -> td.partitions().forEach(pi -> {
            if (pi.leader() != null) {
                mirrorCache.updateSourceLeader(mirrorName, new TopicPartition(td.name(), pi.partition()),
                        new SourceLeader(pi.leader(), pi.leaderEpoch().orElse(0)));
            }
        }));
    }

    /**
     * Validates that all partitions about to be mirrored are in STOPPED state on the source cluster,
     * for any source mirror that was previously mirroring from this local cluster. This prevents
     * starting replication while the reverse direction is still active.
     *
     * @param sourceDescription described mirrors from the source cluster
     * @param sourceMirrors listed mirrors from the source cluster
     * @param tp partitions about to start mirroring
     * @throws IllegalStateException if any partition is not STOPPED
     */
    private void validateSourcePartitionIsStopped(
            Map<String, ClusterMirrorDescription> sourceDescription,
            Collection<ClusterMirrorListing> sourceMirrors,
            TopicPartition tp) {
        List<String> localClusterSourceMirrors = sourceMirrors.stream()
                .filter(sm -> sm.sourceClusterId().equals(metadataManager.clusterId()))
                .map(ClusterMirrorListing::mirrorName)
                .toList();

        for (String mirrorName : localClusterSourceMirrors) {
            ClusterMirrorDescription desc = sourceDescription.get(mirrorName);
            if (desc == null) {
                continue;
            }
            Set<ClusterMirrorDescription.LeaderStateDescription> leaderStates = desc.topics().get(tp.topic());
            if (leaderStates == null) {
                continue;
            }
            boolean notStopped = leaderStates.stream()
                    .anyMatch(lsd -> lsd.topicPartition().equals(tp)
                            && (!MirrorPartitionState.STOPPED.name().equals(lsd.state())));
            if (notStopped) {
                log.error("Source mirror(s) {} mirroring from this cluster ({}) have not stopped for partition {}",
                        localClusterSourceMirrors, metadataManager.clusterId(), tp);
                throw new IllegalStateException("Source mirror(s) " + localClusterSourceMirrors
                        + " mirroring from this cluster (" + metadataManager.clusterId() + ") have not stopped for partition "
                        + tp + ".");
            }
        }
    }

    /** Looks up last mirror epochs from the source cluster for failback truncation. */
    CompletionStage<Map<TopicPartition, Integer>> sendLastMirrorEpochLookup(
            String mirrorName, TopicPartition tp, Collection<ClusterMirrorListing> sourceMirrors) {
        Admin admin = metadataManager.getOrCreateSourceAdmin(mirrorName);
        List<DescribeClusterMirrorsRequestData.LastMirrorEpochLookup> lookups = buildLastMirrorEpochLookup(tp);
        log.info("Last mirror epoch lookup request for mirror {}: {}", mirrorName, lookups);
        DescribeClusterMirrorsOptions options = new DescribeClusterMirrorsOptions()
                .clusterId(metadataManager.clusterId())
                .lastMirrorEpochLookups(lookups);
        DescribeClusterMirrorsResult result = admin.describeClusterMirrors(List.of(), options);

        var describeFuture = result.allDescriptions().toCompletionStage().toCompletableFuture();
        var lookupEpochsFuture = result.lookupEpochs().toCompletionStage().toCompletableFuture();
        return describeFuture.thenApply(desc -> {
            validateSourcePartitionIsStopped(desc, sourceMirrors, tp);
            return null;
        })
            .thenCompose(__ -> lookupEpochsFuture)
            .thenApply(lookupEpochs -> {
                Map<TopicPartition, Integer> epochs = new HashMap<>();
                if (!lookupEpochs.isEmpty()) {
                    lookupEpochs.forEach((topicId, partitionEpochs) -> {
                        Optional<String> topicName = metadataCache.getTopicName(topicId);
                        topicName.ifPresent(name ->
                                partitionEpochs.forEach((partIdx, lme) ->
                                        epochs.put(new TopicPartition(name, partIdx), lme)));
                    });
                }
                log.info("Last mirror epoch lookup response for mirror {}: {}", mirrorName, epochs);
                return epochs;
            })
            .orTimeout(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * Builds LME lookup entries with only this cluster's own ID.
     * The source matches this against its mirror configs to find cases
     * where it previously mirrored from us (direct failback).
     */
    private List<DescribeClusterMirrorsRequestData.LastMirrorEpochLookup> buildLastMirrorEpochLookup(
            TopicPartition tp) {
        Uuid topicId = metadataCache.getTopicId(tp.topic());
        return List.of(new DescribeClusterMirrorsRequestData.LastMirrorEpochLookup()
                .setTopicId(topicId)
                .setPartitions(List.of(tp.partition())));
    }

    record TimeoutHandler(Logger log) implements ControllerRequestCompletionHandler {
        @Override
        public void onTimeout() {
            log.warn("Controller request timed out");
        }

        @Override
        public void onComplete(ClientResponse response) {
            log.debug("Controller request completed: {}", response);
        }
    }

    record SourceTopicState(String topic, Uuid topicId, boolean exists, List<SourcePartitionState> partitions) { }
    record SourcePartitionState(TopicPartition topicPartition, Node leader, Optional<Integer> leaderEpoch) { }
    record SourceAclChanges(List<AclBinding> aclsToAdd, List<AclBinding> aclsToDelete) { }
}
