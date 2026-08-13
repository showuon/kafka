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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.mirror.CoreBridge;
import org.apache.kafka.coordinator.mirror.MirrorPartition;
import org.apache.kafka.coordinator.mirror.MirrorPartitionKey;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Optional;
import java.util.function.Function;

public class CoreBridgeImpl implements CoreBridge {
    private final MirrorMetadataManager metadataManager;
    private final MetadataCache metadataCache;

    public CoreBridgeImpl(MirrorMetadataManager metadataManager, MetadataCache metadataCache) {
        this.metadataManager = metadataManager;
        this.metadataCache = metadataCache;
    }

    @Override
    public void initialize(
        CoordinatorWriter coordinatorWriter,
        CoordinatorReader coordinatorReader,
        Function<MirrorPartitionKey, Integer> coordPartFinder
    ) {
        metadataManager.initialize(coordinatorWriter, coordinatorReader, coordPartFinder);
    }

    @Override
    public void closeSourceAdmins() {
        metadataManager.closeSourceAdmins();
    }

    @Override
    public void onShardLoaded(int coordPartition) {
        metadataManager.onShardLoaded(coordPartition);
    }

    @Override
    public void onShardUnloaded(int coordPartition, int coordPartitionCount) {
        metadataManager.onShardUnloaded(coordPartition, coordPartitionCount);
    }

    @Override
    public MirrorPartition getPartition(MirrorPartitionKey key) {
        return metadataManager.getPartition(key);
    }

    @Override
    public void setPartition(MirrorPartitionKey key, MirrorPartition partition) {
        metadataManager.setPartition(key, partition);
    }

    @Override
    public void removePartition(MirrorPartitionKey key) {
        metadataManager.removePartition(key);
    }

    @Override
    public void updateFailedInfo(
        MirrorPartitionKey key,
        MirrorPartitionState curState,
        MirrorPartitionState newState,
        String errorMessage,
        boolean isPermFailure
    ) {
        metadataManager.updateFailedInfo(key, curState, newState, errorMessage, isPermFailure);
    }

    @Override
    public void setLastMirrorEpoch(String mirrorName, String topic, int partition, int epoch) {
        metadataManager.setLastMirrorEpoch(mirrorName, topic, partition, epoch);
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
