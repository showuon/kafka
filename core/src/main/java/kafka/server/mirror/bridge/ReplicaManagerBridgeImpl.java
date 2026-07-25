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

import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MirrorPidResetRecord;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import scala.jdk.javaapi.CollectionConverters;

public class ReplicaManagerBridgeImpl implements org.apache.kafka.coordinator.mirror.bridge.ReplicaManagerBridge {
    private final ReplicaManager replicaManager;

    public ReplicaManagerBridgeImpl(ReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
    }

    @Override
    public void maybeCreateMirrorFetchers(String mirrorName, Set<TopicPartition> partitions) {
        replicaManager.maybeCreateMirrorFetchers(mirrorName, partitions);
    }

    @Override
    public void removeFetcherForPartitions(Set<TopicPartition> partitions) {
        replicaManager.mirrorFetcherManager().removeFetcherForPartitions(
            CollectionConverters.asScala(partitions));
    }

    @Override
    public OptionalInt getLatestEpoch(TopicPartition tp) {
        var logOpt = replicaManager.getPartitionOrException(tp).log();
        if (logOpt.isDefined()) {
            return OptionalInt.of(logOpt.get().latestEpoch().orElse(-1));
        }
        return OptionalInt.empty();
    }

    @Override
    public Map<TopicPartition, Integer> getLatestLocalEpoch(TopicPartition tp) {
        int epoch = replicaManager.logManager().getLog(tp, false).get().latestEpoch().orElse(-1);
        return Map.of(tp, epoch);
    }

    @Override
    public void maybeTruncateForLeaderEpoch(Map<TopicPartition, Integer> epochs, Consumer<TopicPartition> callback) {
        replicaManager.maybeTruncateForLeaderEpoch(epochs, callback);
    }

    @Override
    public CompletableFuture<Void> abortOngoingTransactions(TopicPartition tp) {
        var record = replicaManager.getLog(tp).map(UnifiedLog::buildEndTransactionRecords);
        if (!record.isDefined() || record.get().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (MemoryRecords memRecords : record.get()) {
            CompletableFuture<Void> batchFuture = new CompletableFuture<>();
            replicaManager.appendRecords(
                    5000L,
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

    @Override
    public CompletableFuture<Void> appendPidResetBarrier(TopicPartition tp, String sourceClusterId, long timestampMs) {
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
                0, timestampMs, 0, buffer, record);
            replicaManager.appendRecords(
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
}
