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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.mirror.MirrorPartition;
import org.apache.kafka.coordinator.mirror.MirrorPartitionKey;
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
        metadataManager.cache().clearPartition(partitionIndex, numPartitions);
    }

    @Override
    public Uuid getTopicId(String topicName) {
        return metadataCache.getTopicId(topicName);
    }

    @Override
    public Optional<String> getTopicName(Uuid topicId) {
        return metadataCache.getTopicName(topicId);
    }

    @Override
    public MirrorPartitionState getPartitionState(String mirrorName, TopicPartition topicPartition) {
        return metadataManager.getPartitionState(mirrorName, topicPartition);
    }

    @Override
    public void setPartitionState(MirrorPartitionKey key, MirrorPartitionState newState) {
        metadataManager.cache().setPartitionState(key, newState);
    }

    @Override
    public void clearPartitionState(MirrorPartitionKey key) {
        metadataManager.cache().remove(key);
    }

    @Override
    public void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch) {
        metadataManager.setLastMirrorEpoch(mirrorName, topic, partition, epoch);
    }

    @Override
    public MirrorPartition getFailedInfo(MirrorPartitionKey key) {
        return metadataManager.cache().get(key);
    }

    @Override
    public void setFailedInfo(MirrorPartitionKey key, MirrorPartition mp) {
        metadataManager.cache().setFailedInfo(key, mp.errorMessage(), mp.retryAttempt(), mp.prevState());
    }

    @Override
    public void updateFailedInfo(MirrorPartitionKey key, MirrorPartitionState currentState,
                                 MirrorPartitionState newState, String errorMessage, boolean nonRetryable) {
        metadataManager.cache().updateFailedInfo(key, currentState, newState, errorMessage, nonRetryable);
    }

    @Override
    public void clearFailedInfo(MirrorPartitionKey key) {
        metadataManager.cache().clearFailedInfo(key);
    }
}
