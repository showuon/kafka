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

import java.util.OptionalInt;

/**
 * Lifecycle interface for the cluster mirror coordinator.
 * Used by BrokerMetadataPublisher and BrokerServer for leader election,
 * resignation, and metadata image updates.
 */
public interface ClusterMirrorCoordinator {
    void start();

    void shutdown();

    void onElection(int partitionIndex, int partitionLeaderEpoch);

    void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch);

    void onNewMetadataImage(
        CoordinatorMetadataImage newImage,
        CoordinatorMetadataDelta delta);
}
