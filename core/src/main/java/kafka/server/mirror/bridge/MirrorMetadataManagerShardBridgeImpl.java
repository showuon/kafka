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
package kafka.server.mirror.bridge;

import kafka.server.mirror.MirrorMetadataManager;
import kafka.server.mirror.MirrorMetadataManager.FailedPartitionInfo;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorShard;
import org.apache.kafka.coordinator.mirror.ClusterMirrorPartitionKey;
import org.apache.kafka.coordinator.mirror.bridge.MirrorMetadataManagerShardBridge;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Optional;

public class MirrorMetadataManagerShardBridgeImpl implements MirrorMetadataManagerShardBridge {
    private final MirrorMetadataManager metadataManager;
    private final MetadataCache metadataCache;

    public MirrorMetadataManagerShardBridgeImpl(MirrorMetadataManager metadataManager, MetadataCache metadataCache) {
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
