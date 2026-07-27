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

import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;

import java.util.OptionalInt;

/**
 * Lifecycle interface for the cluster mirror coordinator.
 * Used by BrokerMetadataPublisher and BrokerServer for leader election,
 * resignation, and metadata image updates.
 */
public interface ClusterMirrorCoordinator {
    /** Activates the coordinator and initializes its dependencies. */
    void startup();

    /** Shuts down the coordinator, releasing all resources. */
    void shutdown();

    /** Called when this broker becomes leader for a {@code __mirror_state} partition. */
    void onElection(int partitionIndex, int partitionLeaderEpoch);

    /** Called when this broker loses leadership of a {@code __mirror_state} partition. */
    void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch);

    /**
     * Reacts to a KRaft metadata delta: tears down changed mirror connections,
     * detects leadership gains and losses, and drives mirror partition state transitions.
     */
    void onMetadataUpdate(MetadataImage newImage, MetadataDelta delta);

    /**
     * Propagates the new metadata snapshot to the CoordinatorRuntime so
     * loaded shards have access to current topic and partition metadata.
     */
    void onNewMetadataImage(
        CoordinatorMetadataImage newImage,
        CoordinatorMetadataDelta delta);
}
