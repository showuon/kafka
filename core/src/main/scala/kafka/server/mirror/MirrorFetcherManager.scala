/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.mirror

import kafka.server._
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig
import org.apache.kafka.server.network.BrokerEndPoint

import scala.collection.{Map, mutable}
import scala.collection.concurrent.TrieMap
import scala.jdk.OptionConverters._

/**
 * Manages {@link MirrorFetcherThread}s, assigning partitions from different mirrors
 * to separate threads for authentication, configuration, and load balancing isolation.
 */
class MirrorFetcherManager(brokerConfig: KafkaConfig,
                           protected val replicaManager: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quotaManager: ReplicationQuotaManager,
                           metadataVersionSupplier: () => MetadataVersion,
                           brokerEpochSupplier: () => Long,
                           metadataCache: MetadataCache)
    extends AbstractFetcherManager[MirrorFetcherThread](
      name = "MirrorFetcherManager id=" + brokerConfig.brokerId,
      clientId = "MirrorReplica",
      numFetchers = brokerConfig.mirrorConfig.numReplicaFetchers) {
  private lazy val mirrorFetcherThreadMap = new mutable.HashMap[MirrorFetcherKey, MirrorFetcherThread]
  private val mirrorOffsetInfoMap = new TrieMap[MirrorTopicPartition, MirrorOffsetInfo]

  override def deadThreadCount: Int = lock synchronized { mirrorFetcherThreadMap.values.count(_.isThreadFailed) }

  override def minFetchRate: Double = {
    // Current min fetch rate across all fetchers/topics/partitions
    val headRate = mirrorFetcherThreadMap.values.headOption.map(_.fetcherStats.requestRate.oneMinuteRate).getOrElse(0.0)
    mirrorFetcherThreadMap.values.foldLeft(headRate)((curMinAll, fetcherThread) =>
      math.min(curMinAll, fetcherThread.fetcherStats.requestRate.oneMinuteRate))
  }

  override def maxLag: Long = {
    // Current max lag across all fetchers/topics/partitions
    mirrorFetcherThreadMap.values.foldLeft(0L) { (curMaxLagAll, fetcherThread) =>
      val maxLagThread = fetcherThread.fetcherLagStats.stats.values.stream().mapToLong(v => v.lag).max().orElse(0L)
      math.max(curMaxLagAll, maxLagThread)
    }
  }

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): MirrorFetcherThread = {
    throw new UnsupportedOperationException("Use createFetcherThread for mirror fetchers")
  }

  override def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    if (isClosed) {
      return
    }

    logger.debug("Adding fetcher for partitions, existing fetchers: {}", mirrorFetcherThreadMap.keys)
    // Ensures partitions with different cluster mirrors get separate fetcher threads.
    // This is crucial because different cluster mirrors may require different authentication credentials.
    val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
      MirrorFetcherKey(
        getFetcherId(topicPartition),
        brokerAndInitialFetchOffset.leader,
        brokerAndInitialFetchOffset.mirrorName
      )
    }

    this.synchronized {
      if (isClosed) {
        return
      }

      def addAndStartFetcherThread(fetcherKey: MirrorFetcherKey): MirrorFetcherThread = {
        val fetcherThread = createFetcherThread(fetcherKey.fetcherId, fetcherKey.sourceBroker, fetcherKey.mirrorName)
        mirrorFetcherThreadMap.put(fetcherKey, fetcherThread)
        fetcherThread.start()
        fetcherThread
      }

      for ((remoteMirrorFetcherKey, initialFetchOffsets) <- partitionsPerFetcher) {
        val fetcherThread = mirrorFetcherThreadMap.get(remoteMirrorFetcherKey) match {
          case Some(currentFetcherThread) if currentFetcherThread.leader.brokerEndPoint() == remoteMirrorFetcherKey.sourceBroker =>
            // Reuse the fetcher thread
            logger.debug("Reusing mirror fetcher for {}", remoteMirrorFetcherKey)
            currentFetcherThread
          case Some(f) =>
            logger.debug("Recreating mirror fetcher for {}", remoteMirrorFetcherKey)
            f.shutdown()
            addAndStartFetcherThread(remoteMirrorFetcherKey)
          case None =>
            logger.debug("Creating new mirror fetcher for {}", remoteMirrorFetcherKey)
            addAndStartFetcherThread(remoteMirrorFetcherKey)
        }
        // Failed partitions are removed when added partitions to thread
        addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)

        // Initialize lag information for newly added partitions
        initialFetchOffsets.foreach { case (topicPartition, initialState) =>
          val lagKey = MirrorTopicPartition(remoteMirrorFetcherKey.mirrorName, topicPartition)
          // Initialize with 0 values until first fetch updates it
          val destinationOffset = replicaManager.getPartition(topicPartition) match {
            case HostedPartition.Online(partition) =>
              partition.log.map(_.highWatermark).getOrElse(0L)
            case _ => 0L
          }
          mirrorOffsetInfoMap.put(lagKey, MirrorOffsetInfo(destinationOffset, destinationOffset, time.milliseconds()))
        }
      }
    }
  }

  private def createFetcherThread(fetcherId: Int, srcEndpoint: BrokerEndPoint, mirrorName: String): MirrorFetcherThread = {
    info(s"Creating mirror fetcher thread: fetcherId = $fetcherId, srcEndpoint = $srcEndpoint, mirrorName = $mirrorName")
    val threadName = s"MirrorFetcherThread-$fetcherId-${srcEndpoint.id}-$mirrorName"
    val logContext = new LogContext(s"[MirrorFetcher id=${brokerConfig.brokerId}, fetcherId=$fetcherId, leaderId=${srcEndpoint.id}, mirrorName=$mirrorName] ")

    if (mirrorName.isEmpty) {
      throw new IllegalArgumentException("Mirror name must be provided for remote fetchers")
    }
    val mirrorProperties = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName))
    info(s"Using mirror properties for $mirrorName: ${mirrorProperties.keySet()}")
    val mirrorConfig = ClusterMirrorConfig.fromProperties(mirrorProperties)
    val clientId = s"fetcherId-$fetcherId-mirrorName-$mirrorName"
    val sender = new MirrorSourceSender(srcEndpoint, mirrorConfig, metrics, time, srcEndpoint.id, clientId, logContext)
    val fetchSessionHandler = new FetchSessionHandler(logContext, srcEndpoint.id)
    val endpoint: LeaderEndPoint = new RemoteLeaderEndPoint(logContext.logPrefix, sender, fetchSessionHandler, brokerConfig,
      replicaManager, quotaManager, metadataVersionSupplier, brokerEpochSupplier, isClusterMirror = true,
      mirrorConfig = Some(mirrorConfig))
    val mirrorFetchBackoffMs = mirrorConfig.fetchBackoffMs().toInt
    new MirrorFetcherThread(threadName, endpoint, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix, mirrorName, mirrorFetchBackoffMs)
  }

  override def removeFetcherForPartitions(partitions: scala.collection.Set[TopicPartition]): scala.collection.Map[TopicPartition, PartitionFetchState] = {
    val fetchStates = mutable.Map.empty[TopicPartition, PartitionFetchState]
    this.synchronized {
      for ((key, fetcher) <- mirrorFetcherThreadMap) {
        val removed = fetcher.removePartitions(partitions)
        fetchStates ++= removed
        // Remove lag cache entries for partitions that were actually removed
        for (partition <- removed.keys) {
          val lagKey = MirrorTopicPartition(key.mirrorName, partition)
          mirrorOffsetInfoMap.remove(lagKey)
        }
      }
      failedPartitions.removeAll(partitions)
    }
    // Only log if we actually removed mirror partitions (not regular partitions)
    if (fetchStates.nonEmpty)
      info(s"Removed mirror fetcher for partitions ${fetchStates.keySet}")
    fetchStates
  }

  // Collect idle fetchers under lock, shut down outside to avoid deadlock
  override def shutdownIdleFetcherThreads(): Unit = {
    val idleFetchers = this.synchronized {
      val keysToBeRemoved = new mutable.HashSet[MirrorFetcherKey]
      val fetchersToShutdown = new mutable.ArrayBuffer[MirrorFetcherThread]
      for ((key, fetcher) <- mirrorFetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
          fetchersToShutdown += fetcher
          keysToBeRemoved += key
        }
      }
      mirrorFetcherThreadMap --= keysToBeRemoved
      fetchersToShutdown
    }
    idleFetchers.foreach(_.shutdown())
  }

  override def resizeThreadPool(newSize: Int): Unit = {
    val excessThreads = new mutable.ArrayBuffer[MirrorFetcherThread]()
    this.synchronized {
      if (isClosed) return
      val currentSize = updateNumFetchers(newSize)
      if (newSize == currentSize) return
      info(s"Resizing mirror fetcher thread pool from $currentSize to $newSize")
      val allPartitions = mutable.Map[TopicPartition, InitialFetchState]()
      for ((key, thread) <- mirrorFetcherThreadMap) {
        val partitionStates = thread.removeAllPartitions()
        if (key.fetcherId >= newSize) {
          thread.initiateShutdown()
          excessThreads += thread
        }
        partitionStates.foreachEntry { (topicPartition, state) =>
          allPartitions += topicPartition -> InitialFetchState(state.topicId.toScala,
            thread.leader.brokerEndPoint(),
            currentLeaderEpoch = state.currentLeaderEpoch,
            initOffset = state.fetchOffset,
            mirrorName = state.mirrorName())
        }
      }
      mirrorFetcherThreadMap.filterInPlace((key, _) => key.fetcherId < newSize)
      addFetcherForPartitions(allPartitions)
    }
    shutdownIdleFetcherThreads()
    excessThreads.foreach(_.shutdown())
  }

  override def closeAllFetchers(): Unit = {
    val fetchers = this.synchronized {
      isClosed = true
      val all = mirrorFetcherThreadMap.values.toSeq
      all.foreach(_.initiateShutdown())
      mirrorFetcherThreadMap.clear()
      all
    }
    fetchers.foreach(_.shutdown())
  }

  def updateMirrorOffsetInfo(mirrorName: String, topicPartition: TopicPartition, sourceOffset: Long, destinationOffset: Long): Unit = {
    val key = MirrorTopicPartition(mirrorName, topicPartition)
    mirrorOffsetInfoMap.put(key, MirrorOffsetInfo(sourceOffset, destinationOffset, time.milliseconds()))
  }

  def getMirrorOffsetInfo(mirrorName: String): Map[TopicPartition, MirrorOffsetInfo] = {
    mirrorOffsetInfoMap.collect {
      case (key, info) if key.mirrorName == mirrorName => key.topicPartition -> info
    }.toMap
  }

  def removeFetchersForMirror(mirrorName: String): Unit = {
    this.synchronized {
      val affectedPartitions = mirrorFetcherThreadMap
        .filter(_._1.mirrorName == mirrorName)
        .values
        .flatMap(_.partitions)
        .toSet
      if (affectedPartitions.nonEmpty) {
        info(s"Restarting fetcher threads for mirror '$mirrorName' " +
          s"affecting ${affectedPartitions.size} partitions")
        removeFetcherForPartitions(affectedPartitions)
      }
    }
  }

  def shutdown(): Unit = {
    info("shutting down")
    closeAllFetchers()
    mirrorOffsetInfoMap.clear()
    info("shutdown completed")
  }
}

/**
 * Three-dimensional key for grouping mirror fetcher threads.
 *
 * Multiple partitions share the same fetcher thread when they have identical keys
 * (fetcher ID, source broker, and mirror name). This key determines thread reuse.
 *
 * Example with num.mirror.replica.fetchers = 2:
 *
 * | Partition  | Fetcher ID | Source Leader | Mirror Name | Key                 | Thread Reused? |
 * |------------|------------|---------------|-------------|---------------------|----------------|
 * | topic1-p0  | 0          | broker-1      | A2B         | (0, broker-1, A2B)  | New thread     |
 * | topic1-p1  | 1          | broker-1      | A2B         | (1, broker-1, A2B)  | New thread     |
 * | topic2-p0  | 0          | broker-1      | A2B         | (0, broker-1, A2B)  | Reuse          |
 * | topic2-p1  | 1          | broker-1      | A2B         | (1, broker-1, A2B)  | Reuse          |
 * | topic3-p0  | 0          | broker-2      | A2B         | (0, broker-2, A2B)  | New thread     |
 * | topic4-p0  | 0          | broker-1      | A2C         | (0, broker-1, A2C)  | New thread     |
 */
case class MirrorFetcherKey(fetcherId: Int, sourceBroker: BrokerEndPoint, mirrorName: String)

case class MirrorTopicPartition(mirrorName: String, topicPartition: TopicPartition)

case class MirrorOffsetInfo(sourceOffset: Long, destinationOffset: Long, lastUpdateMs: Long)
