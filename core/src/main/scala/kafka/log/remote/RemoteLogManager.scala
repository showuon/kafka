/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import kafka.cluster.Partition
import kafka.log.UnifiedLog
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common._
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.message.FetchResponseData.AbortedTransaction
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, RemoteLogInputStream}
import org.apache.kafka.common.requests.FetchRequest.{PartitionData}
import org.apache.kafka.common.utils.{ChildFirstClassLoader, KafkaThread, Time, Utils}
import org.apache.kafka.server.common.CheckpointFile.CheckpointWriteBuffer
import org.apache.kafka.server.log.remote.metadata.storage.ClassLoaderAwareRemoteLogMetadataManager
import org.apache.kafka.server.log.remote.storage._
import org.apache.kafka.storage.internals.checkpoint.{LeaderEpochCheckpoint, LeaderEpochCheckpointFile}
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{AbortedTxn, EpochEntry, FetchDataInfo, FetchIsolation, LogOffsetMetadata, OffsetPosition, RemoteStorageFetchInfo}

import java.io.{BufferedWriter, ByteArrayOutputStream, Closeable, File, InputStream, OutputStreamWriter}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.{AccessController, PrivilegedAction}
import java.util
import java.util.{Optional, OptionalInt}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Future, ScheduledFuture, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import scala.collection.Searching.{Found, InsertionPoint}
import scala.collection.Set
import scala.jdk.CollectionConverters._

/**
 * This class is responsible for
 *  - initializing `RemoteStorageManager` and `RemoteLogMetadataManager` instances.
 *  - receives any leader and follower replica events and partition stop events and act on them
 *  - also provides APIs to fetch indexes, metadata about remote log segments.
 *
 * @param rlmConfig Configuration required for remote logging subsystem(tiered storage) at the broker level.
 * @param brokerId  id of the current broker.
 * @param logDir    directory of Kafka log segments.
 */
class RemoteLogManager(fetchLog: TopicIdPartition => Option[UnifiedLog],
                       updateRemoteLogStartOffset: (TopicPartition, Long) => Unit,
                       time: Time = Time.SYSTEM,
                       rlmConfig: RemoteLogManagerConfig,
                       brokerId: Int,
                       logDir: String) extends Logging with Closeable {
  case class RLMTaskWithFuture(rlmTask: RLMTask, future: Future[_]) {
    def cancel(): Unit = {
      rlmTask.cancel()
      try {
        future.cancel(true)
      } catch {
        case ex: Exception => error(s"Error occurred while canceling the task: $rlmTask", ex)
      }
    }
  }


  private val leaderOrFollowerTasks: ConcurrentHashMap[TopicIdPartition, RLMTaskWithFuture] =
    new ConcurrentHashMap[TopicIdPartition, RLMTaskWithFuture]()

  // topic ids received on leadership changes
  private val topicPartitionIds: ConcurrentMap[TopicPartition, Uuid] = new ConcurrentHashMap[TopicPartition, Uuid]()

  private val remoteLogStorageManager: RemoteStorageManager = createRemoteStorageManager()
  private val remoteLogMetadataManager: RemoteLogMetadataManager = createRemoteLogMetadataManager()

  private val indexCache = new RemoteIndexCache(remoteStorageManager = remoteLogStorageManager, logDir = logDir)

  private var closed = false

  private val delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs
  private val poolSize = rlmConfig.remoteLogManagerThreadPoolSize
  private val rlmScheduledThreadPool = new RLMScheduledThreadPool(poolSize)

  private val remoteStorageFetcherThreadPool = new RemoteStorageReaderThreadPool(rlmConfig.remoteLogReaderThreads,
    rlmConfig.remoteLogReaderMaxPendingTasks, time)


  private[remote] def createRemoteStorageManager(): RemoteStorageManager = {
    def createDelegate(classLoader: ClassLoader): RemoteStorageManager = {
      classLoader.loadClass(rlmConfig.remoteStorageManagerClassName())
        .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
    }

    AccessController.doPrivileged(new PrivilegedAction[RemoteStorageManager] {
      private val classPath = rlmConfig.remoteStorageManagerClassPath()

      override def run(): RemoteStorageManager = {
          if (classPath != null && classPath.trim.nonEmpty) {
            val classLoader = new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
            val delegate = createDelegate(classLoader)
            new ClassLoaderAwareRemoteStorageManager(delegate, classLoader)
          } else {
            createDelegate(this.getClass.getClassLoader)
          }
      }
    })
  }

  private def doHandleLeaderOrFollowerPartitions(topicPartition: TopicIdPartition,
                                                 convertToLeaderOrFollower: RLMTask => Unit): Unit = {
    var conversionRequired = true
    val rlmTaskWithFuture = leaderOrFollowerTasks.computeIfAbsent(topicPartition, (tp: TopicIdPartition) => {
      val task = new RLMTask(tp)
      // set this upfront when it is getting initialized instead of doing it after scheduling.
      convertToLeaderOrFollower(task)
      conversionRequired = false
      info(s"Created a new task: $task and getting scheduled")
      val future = rlmScheduledThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS)
      RLMTaskWithFuture(task, future)
    })
    if (conversionRequired) {
      convertToLeaderOrFollower(rlmTaskWithFuture.rlmTask)
    }
  }

  private def configureRLMM(endPoint: Endpoint): Unit = {
    val rlmmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteLogMetadataManagerProps().asScala.foreach { case (k, v) => rlmmProps.put(k, v) }
    rlmmProps.put(KafkaConfig.LogDirProp, logDir)
    rlmmProps.put(KafkaConfig.BrokerIdProp, brokerId)
//    rlmmProps.put("cluster.id", )
    rlmmProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, endPoint.host + ":" + endPoint.port)
    rlmmProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, endPoint.securityProtocol.name)
    remoteLogMetadataManager.configure(rlmmProps)
  }

  def onEndpointCreated(serverEndPoint: Endpoint): Unit = {
    // initialize and configure RSM and RLMM
    configureRSM()
    configureRLMM(serverEndPoint)
  }

  private def configureRSM(): Unit = {
    val rsmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageManagerProps().asScala.foreach { case (k, v) => rsmProps.put(k, v) }
    rsmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    remoteLogStorageManager.configure(rsmProps)
  }

  private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = {
    def createDelegate(classLoader: ClassLoader) = {
      classLoader.loadClass(rlmConfig.remoteLogMetadataManagerClassName())
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[RemoteLogMetadataManager]
    }

    AccessController.doPrivileged(new PrivilegedAction[RemoteLogMetadataManager] {
      private val classPath = rlmConfig.remoteLogMetadataManagerClassPath

      override def run(): RemoteLogMetadataManager = {
        if (classPath != null && classPath.trim.nonEmpty) {
          val classLoader = new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
          val delegate = createDelegate(classLoader)
          new ClassLoaderAwareRemoteLogMetadataManager(delegate, classLoader)
        } else {
          createDelegate(this.getClass.getClassLoader)
        }
      }
    })
  }

  private def configureRLMM(): Unit = {
    val rlmmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteLogMetadataManagerProps().asScala.foreach { case (k, v) => rlmmProps.put(k, v) }
    rlmmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    rlmmProps.put(KafkaConfig.LogDirProp, logDir)
    remoteLogMetadataManager.configure(rlmmProps)
  }

  def startup(): Unit = {
    // Initialize and configure RSM and RLMM. This will start RSM, RLMM resources which may need to start resources
    // in connecting to the brokers or remote storages.
    configureRSM()
    configureRLMM()
  }

  def storageManager(): RemoteStorageManager = {
    remoteLogStorageManager
  }

  /**
   * Callback to receive any leadership changes for the topic partitions assigned to this broker. If there are no
   * existing tasks for a given topic partition then it will assign new leader or follower task else it will convert the
   * task to respective target state(leader or follower).
   *
   * @param partitionsBecomeLeader   partitions that have become leaders on this broker.
   * @param partitionsBecomeFollower partitions that have become followers on this broker.
   * @param topicIds                 topic name to topic id mappings.
   */
  def onLeadershipChange(partitionsBecomeLeader: Set[Partition],
                         partitionsBecomeFollower: Set[Partition],
                         topicIds: util.Map[String, Uuid]): Unit = {
    info(s"!!! Received leadership changes for leaders: $partitionsBecomeLeader and followers: $partitionsBecomeFollower")

    // Partitions logs are available when this callback is invoked.
    // Compact topics and internal topics are filtered here as they are not supported with tiered storage.
    def filterPartitions(partitions: Set[Partition]): Set[TopicIdPartition] = {
      // We are not specifically checking for internal topics etc here as `log.remoteLogEnabled()` already handles that.
      partitions.filter(partition => partition.log.exists(log => log.remoteLogEnabled()))
        .map(partition => new TopicIdPartition(topicIds.get(partition.topic), partition.topicPartition))
    }

    def filterLeaderPartitions(partitions: Set[Partition]): Map[TopicIdPartition, Int] = {
      // We are not specifically checking for internal topics etc here as `log.remoteLogEnabled()` already handles that.
      partitions.filter(partition => partition.log.exists(log => log.remoteLogEnabled()))
        .map(partition => new TopicIdPartition(topicIds.get(partition.topic), partition.topicPartition) -> partition.getLeaderEpoch).toMap
    }

    val followerTopicPartitions = filterPartitions(partitionsBecomeFollower)
    val leaderTopicPartitions = filterLeaderPartitions(partitionsBecomeLeader)
    info(s"Effective topic partitions after filtering compact and internal topics, leaders: $leaderTopicPartitions " +
      s"and followers: $followerTopicPartitions")

    if (leaderTopicPartitions.nonEmpty || followerTopicPartitions.nonEmpty) {
      leaderTopicPartitions.foreach(x => topicPartitionIds.put(x._1.topicPartition(), x._1.topicId()))
      followerTopicPartitions.foreach(x => topicPartitionIds.put(x.topicPartition(), x.topicId()))

      remoteLogMetadataManager.onPartitionLeadershipChanges(leaderTopicPartitions.keySet.asJava, followerTopicPartitions.asJava)

      followerTopicPartitions.foreach {
        topicIdPartition => doHandleLeaderOrFollowerPartitions(topicIdPartition, _.convertToFollower())
      }
      leaderTopicPartitions.foreach {
        case (topicIdPartition, epoch) =>
          doHandleLeaderOrFollowerPartitions(topicIdPartition, _.convertToLeader(epoch))
      }
    }
  }

  /**
   * Deletes the internal topic partition info if delete flag is set as true.
   *
   * @param topicPartition topic partition to be stopped.
   * @param delete         flag to indicate whether the given topic partitions to be deleted or not.
   */
  def stopPartitions(allPartitions: Set[TopicPartition], delete: Boolean, errorHandler: (TopicPartition, Throwable) => Unit): Unit = {
    info(s"Stopping ${allPartitions.size} partitions, delete: $delete")
    val partitionsByTopic = allPartitions.groupBy(_.topic())
    partitionsByTopic.foreachEntry((_, partitions) => {
      // FIXME: When to remove the topicId from topicIds map? (leaving them can lead to memory leak)
      val topicId = topicPartitionIds.get(partitions.head)

      val tpIds = partitions.map(new TopicIdPartition(topicId, _))
      tpIds.foreach(tpId => {
        val partition = tpId.topicPartition()
        try {
          val task = leaderOrFollowerTasks.remove(tpId)
          if (task != null) {
            info(s"Cancelling the RLM task for tp: $partition")
            task.cancel()
          }
          if (delete) {
            debug(s"Deleting the remote log segments for partition: $tpId")
            remoteLogMetadataManager.listRemoteLogSegments(tpId).forEachRemaining(elt => deleteRemoteLogSegment(elt, _ => true))
          }
        } catch {
          case ex: Throwable => errorHandler(partition, ex)
        }
      })
      if (delete) {
        // NOTE: this#stopPartitions method is called when Replica state changes to Offline and ReplicaDeletionStarted
        remoteLogMetadataManager.onStopPartitions(tpIds.asJava)
        // Delete from internal datastructures only if it is to be deleted.
        val topicIdPartition = topicPartitionIds.remove(partitions)
        debug(s"Removed partition: $topicIdPartition from topicPartitionIds")
      }

    })
  }

  private def deleteRemoteLogSegment(segmentMetadata: RemoteLogSegmentMetadata, predicate: RemoteLogSegmentMetadata => Boolean): Boolean = {
    if (predicate(segmentMetadata)) {
      info("passed predicate")
      // Publish delete segment started event.
      remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
        new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
          RemoteLogSegmentState.DELETE_SEGMENT_STARTED, brokerId))

      // Delete the segment in remote storage.
      remoteLogStorageManager.deleteLogSegmentData(segmentMetadata)

      // Publish delete segment finished event.
      remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
        new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
          RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId))
      true
    } else false
  }

  def fetchRemoteLogSegmentMetadata(topicPartition: TopicPartition,
                                    epochForOffset: Int,
                                    offset: Long): Optional[RemoteLogSegmentMetadata] = {
    val topicId = topicPartitionIds.get(topicPartition)

    if (topicId == null) {
      throw new KafkaException("No topic id registered for topic partition: " + topicPartition)
    }

    remoteLogMetadataManager.remoteLogSegmentMetadata(new TopicIdPartition(topicId, topicPartition), epochForOffset, offset)
  }

  private def lookupTimestamp(rlsMetadata: RemoteLogSegmentMetadata, timestamp: Long, startingOffset: Long): Option[TimestampAndOffset] = {
    val startPos = indexCache.lookupTimestamp(rlsMetadata, timestamp, startingOffset)

    var remoteSegInputStream: InputStream = null
    try {
      // Search forward for the position of the last offset that is greater than or equal to the startingOffset
      remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(rlsMetadata, startPos)
      val remoteLogInputStream = new RemoteLogInputStream(remoteSegInputStream)
      var batch: RecordBatch = null

      def nextBatch(): RecordBatch = {
        batch = remoteLogInputStream.nextBatch()
        batch
      }

      while (nextBatch() != null) {
        if (batch.maxTimestamp >= timestamp && batch.lastOffset >= startingOffset) {
          batch.iterator.asScala.foreach(record => {
            if (record.timestamp >= timestamp && record.offset >= startingOffset)
              return Some(new TimestampAndOffset(record.timestamp, record.offset, maybeLeaderEpoch(batch.partitionLeaderEpoch)))
          })
        }
      }
      None
    } finally {
      Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream")
    }
  }

  private def maybeLeaderEpoch(leaderEpoch: Int): Optional[Integer] = {
    if (leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH)
      Optional.empty()
    else
      Optional.of(leaderEpoch)
  }

  /**
   * Search the message offset in the remote storage based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If there are no messages in the remote storage, return None
   * - If all the messages in the remote storage have smaller offsets, return None
   * - If all the messages in the remote storage have smaller timestamps, return None
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   * is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * @param tp               topic partition in which the offset to be found.
   * @param timestamp        The timestamp to search for.
   * @param startingOffset   The starting offset to search.
   * @param leaderEpochCache LeaderEpochFileCache of the topic partition.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there
   *         is no such message.
   */
  def findOffsetByTimestamp(tp: TopicPartition,
                            timestamp: Long,
                            startingOffset: Long,
                            leaderEpochCache: LeaderEpochFileCache): Option[TimestampAndOffset] = {
    val topicId = topicPartitionIds.get(tp)
    if (topicId == null) {
      throw new KafkaException("Topic id does not exist for topic partition: " + tp)
    }

    // Get the respective epoch in which the starting-offset exists.
    var maybeEpoch = leaderEpochCache.epochForOffset(startingOffset)
    while (maybeEpoch.isPresent) {
      val epoch = maybeEpoch.getAsInt
      remoteLogMetadataManager.listRemoteLogSegments(new TopicIdPartition(topicId, tp), epoch).asScala
        .foreach(rlsMetadata =>
          if (rlsMetadata.maxTimestampMs() >= timestamp && rlsMetadata.endOffset() >= startingOffset) {
            val timestampOffset = lookupTimestamp(rlsMetadata, timestamp, startingOffset)
            if (timestampOffset.isDefined)
              return timestampOffset
          }
        )

      // Move to the next epoch if not found with the current epoch.
      maybeEpoch = leaderEpochCache.nextEpoch(epoch)
    }
    None
  }

  /**
   * A remote log read task returned by asyncRead(). The caller of asyncRead() can use this object to cancel a
   * pending task or check if the task is done.
   */
  case class AsyncReadTask(future: Future[Unit]) {
    def cancel(mayInterruptIfRunning: Boolean): Boolean = {
      val r = future.cancel(mayInterruptIfRunning)
      if (r) {
        // Removed the cancelled task from task queue
        remoteStorageFetcherThreadPool.purge()
      }
      r
    }

    def isCancelled: Boolean = future.isCancelled

    def isDone: Boolean = future.isDone
  }

  /**
   * Submit a remote log read task.
   *
   * This method returns immediately. The read operation is executed in a thread pool.
   * The callback will be called when the task is done.
   *
   * @throws RejectedExecutionException if the task cannot be accepted for execution (task queue is full)
   */
  def asyncRead(fetchInfo: RemoteStorageFetchInfo, callback: RemoteLogReadResult => Unit): AsyncReadTask = {
    info("!!! asyncRead:" + fetchInfo)
    AsyncReadTask(remoteStorageFetcherThreadPool.submit(new RemoteLogReader(fetchInfo, this, null, callback)))
  }

  /**
   * Closes and releases all the resources like RemoterStorageManager and RemoteLogMetadataManager.
   */
  def close(): Unit = {
    this synchronized {
      if (!closed) {
        Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager")
        Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager")
        Utils.closeQuietly(indexCache, "RemoteIndexCache")
        closed = true
      }
    }
  }

  def findHighestRemoteOffset(topicIdPartition: TopicIdPartition): Long = {
    var offset: Optional[java.lang.Long] = Optional.empty()
    fetchLog(topicIdPartition).foreach { log =>
      log.leaderEpochCache.foreach(cache => {
        var epoch = cache.latestEpoch
        while (!offset.isPresent && epoch.isPresent) {
          offset = remoteLogMetadataManager.highestOffsetForEpoch(topicIdPartition, epoch.getAsInt)
          epoch = cache.previousEpoch(epoch.getAsInt)
        }
      })
    }
    offset.orElse(-1L)
  }

  class InMemoryLeaderEpochCheckpoint extends LeaderEpochCheckpoint {
    private val epochs: util.List[EpochEntry] = new util.ArrayList[EpochEntry]()

    override def write(epochs: util.Collection[EpochEntry]): Unit = {

      this.epochs.addAll(epochs)
    }

    override def read(): util.List[EpochEntry] = {
      this.epochs
    }

    def readAsByteBuffer(): ByteBuffer = {
      val stream = new ByteArrayOutputStream()
      val writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))
      val writeBuffer = new CheckpointWriteBuffer[EpochEntry](writer, 0, new LeaderEpochCheckpointFile.Formatter())
      try {
        writeBuffer.write(epochs)
        writer.flush()
        ByteBuffer.wrap(stream.toByteArray)
      } finally {
        writer.close()
      }
    }
  }

  /**
   * Returns the leader epoch checkpoint by truncating with the given start[exclusive] and end[inclusive] offset
   *
   * @param log         The actual log from where to take the leader-epoch checkpoint
   * @param startOffset The start offset of the checkpoint file (exclusive in the truncation).
   *                    If start offset is 6, then it will retain an entry at offset 6.
   * @param endOffset   The end offset of the checkpoint file (inclusive in the truncation)
   *                    If end offset is 100, then it will remove the entries greater than or equal to 100.
   * @return the truncated leader epoch checkpoint
   */
  private[remote] def getLeaderEpochCheckpoint(log: UnifiedLog, startOffset: Long, endOffset: Long): InMemoryLeaderEpochCheckpoint = {
    val checkpoint = new InMemoryLeaderEpochCheckpoint()
    log.leaderEpochCache
      .map(cache => cache.writeTo(checkpoint))
      .foreach { x =>
        if (startOffset >= 0) {
          x.truncateFromStart(startOffset)
        }
        x.truncateFromEnd(endOffset)
      }
    checkpoint
  }

  def lookupPositionForOffset(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, offset: Long): Int = {
    indexCache.lookupOffset(remoteLogSegmentMetadata, offset)
  }

  def read(remoteStorageFetchInfo: RemoteStorageFetchInfo): FetchDataInfo = {
    info("!!! read:" + remoteStorageFetchInfo)


    val fetchMaxBytes = remoteStorageFetchInfo.fetchMaxBytes
    val tp = remoteStorageFetchInfo.topicPartition
    val fetchInfo: PartitionData = remoteStorageFetchInfo.fetchInfo

    val includeAbortedTxns = remoteStorageFetchInfo.fetchIsolation == FetchIsolation.TXN_COMMITTED

    val offset = fetchInfo.fetchOffset
    val maxBytes = Math.min(fetchMaxBytes, fetchInfo.maxBytes)

    // get the epoch for the requested  offset from local leader epoch cache
    // FIXME(@kamal), use the epochForOffset API instead of latest epoch.
    //  val epoch = fetchLog(tp).map(log => log.leaderEpochCache.map(cache => cache.epochForOffset()))
    var rlsMetadata: Optional[RemoteLogSegmentMetadata] = Optional.empty()
    fetchLog(tp).foreach { log =>
      log.leaderEpochCache.foreach(cache => {
        var epoch = cache.latestEpoch
        while (!rlsMetadata.isPresent && epoch.isPresent) {
          rlsMetadata = fetchRemoteLogSegmentMetadata(tp.topicPartition(), epoch.getAsInt, offset)
          epoch = cache.previousEpoch(epoch.getAsInt)
        }
      })
    }

    if (!rlsMetadata.isPresent) {
      throw new OffsetOutOfRangeException(
        s"Received request for offset $offset for partition $tp which does not exist in remote tier. Try again later.")
    }

    val startPos = lookupPositionForOffset(rlsMetadata.get(), offset)
    var remoteSegInputStream: InputStream = null
    try {
      // Search forward for the position of the last offset that is greater than or equal to the target offset
      remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(rlsMetadata.get(), startPos)
      val remoteLogInputStream = new RemoteLogInputStream(remoteSegInputStream)

      def findFirstBatch(): RecordBatch = {
        var nextBatch: RecordBatch = null

        def iterateNextBatch(): RecordBatch = {
          nextBatch = remoteLogInputStream.nextBatch()
          nextBatch
        }
        // Look for the batch which has the desired offset
        // we will always have a batch in that segment as it is a non-compacted topic. For compacted topics, we may need
        //to read from the subsequent segments if there is no batch available for the desired offset in the current
        //segment. That means, desired offset is more than last offset of the current segment and immediate available
        //offset exists in the next segment which can be higher than the desired offset.
        while (iterateNextBatch() != null && nextBatch.lastOffset < offset) {
        }
        nextBatch
      }

      val firstBatch = findFirstBatch()

      if (firstBatch == null)
        return new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY, false,
          if (includeAbortedTxns) Optional.of(util.Collections.emptyList[org.apache.kafka.common.message.FetchResponseData.AbortedTransaction]()) else Optional.empty(),
          Optional.empty())

      val updatedFetchSize =
        if (remoteStorageFetchInfo.minOneMessage && firstBatch.sizeInBytes() > maxBytes) firstBatch.sizeInBytes()
        else maxBytes

      val buffer = ByteBuffer.allocate(updatedFetchSize)
      var remainingBytes = updatedFetchSize

      firstBatch.writeTo(buffer)
      remainingBytes -= firstBatch.sizeInBytes()

      if (remainingBytes > 0) {
        // input stream is read till (startPos - 1) while getting the batch of records earlier.
        // read the input stream until min of (EOF stream or buffer's remaining capacity).
        Utils.readFully(remoteSegInputStream, buffer)
      }
      buffer.flip()

      var fetchDataInfo = new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.readableRecords(buffer))
      if (includeAbortedTxns) {
        fetchDataInfo = addAbortedTransactions(firstBatch.baseOffset(), rlsMetadata.get(), fetchDataInfo)
      }
      fetchDataInfo
    } finally {
      Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream")
    }
  }


  private[remote] def addAbortedTransactions(startOffset: Long,
                                             segmentMetadata: RemoteLogSegmentMetadata,
                                             fetchInfo: FetchDataInfo): FetchDataInfo = {
    val fetchSize = fetchInfo.records.sizeInBytes
    val startOffsetPosition = new OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)

    val offsetIndex = indexCache.getIndexEntry(segmentMetadata).offsetIndex
    val upperBoundOffset = offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize)
      .map(_.offset).orElse(segmentMetadata.endOffset() + 1)

    val abortedTransactions = new util.ArrayList[AbortedTransaction]

    def accumulator(abortedTxn: util.List[AbortedTxn]): Unit = abortedTxn.forEach( e => abortedTransactions.add(e.asAbortedTransaction()) )

    collectAbortedTransactions(startOffset, upperBoundOffset, segmentMetadata, accumulator)

    new FetchDataInfo(fetchInfo.fetchOffsetMetadata,
      fetchInfo.records,
      fetchInfo.firstEntryIncomplete,
      Optional.of(abortedTransactions))
  }

  private[remote] def collectAbortedTransactions(startOffset: Long,
                                                 upperBoundOffset: Long,
                                                 segmentMetadata: RemoteLogSegmentMetadata,
                                                 accumulator: util.List[AbortedTxn] => Unit): Unit = {
    val topicIdPartition = segmentMetadata.topicIdPartition()
    val localLogSegments = fetchLog(topicIdPartition).map(log => log.logSegments.iterator).getOrElse(Iterator.empty)

    var searchInLocalLog = false
    var nextSegmentMetadataOpt = Option.apply(segmentMetadata)
    var txnIndexOpt = nextSegmentMetadataOpt.map(metadata => indexCache.getIndexEntry(metadata).txnIndex)
    while (txnIndexOpt.isDefined) {
      val searchResult = txnIndexOpt.get.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions)
      if (!searchResult.isComplete) {
        if (!searchInLocalLog) {
          nextSegmentMetadataOpt = nextSegmentMetadataOpt.flatMap(x => findNextSegmentMetadata(x))
          txnIndexOpt = nextSegmentMetadataOpt.map(x => indexCache.getIndexEntry(x).txnIndex)
          if (txnIndexOpt.isEmpty) {
            searchInLocalLog = true
          }
        }
        if (searchInLocalLog) {
          txnIndexOpt = if (localLogSegments.hasNext) Some(localLogSegments.next().txnIndex) else None
        }
      } else {
        return
      }
    }
  }

  private[remote] def findNextSegmentMetadata(segmentMetadata: RemoteLogSegmentMetadata): Option[RemoteLogSegmentMetadata] = {
    val topicIdPartition = segmentMetadata.topicIdPartition()
    val nextSegmentBaseOffset = segmentMetadata.endOffset() + 1
    var epoch = OptionalInt.of(segmentMetadata.segmentLeaderEpochs().lastEntry().getKey.toInt)
    var result: Option[RemoteLogSegmentMetadata] = Option.empty;
    fetchLog(topicIdPartition).foreach(log => {
      log.leaderEpochCache.foreach(cache => {
        while (result.isEmpty && epoch.isPresent) {
          result = Option(fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), epoch.getAsInt, nextSegmentBaseOffset).orElse(null))
          epoch = cache.nextEpoch(epoch.getAsInt)
        }
      })
    })
    result
  }


  //----
  class RLMTask(tpId: TopicIdPartition) extends CancellableRunnable with Logging {
    this.logIdent = s"[RemoteLogManager=$brokerId partition=$tpId] "
    @volatile private var leaderEpoch: Int = -1

    private def isLeader(): Boolean = leaderEpoch >= 0

    // The readOffset is None initially for a new leader RLMTask,
    // and needs to be fetched inside the task's run() method.
    private var readOffsetOption: Option[Long] = None

    //todo-updating log with remote index highest offset -- should this be required?
    // fetchLog(tp.topicPartition()).foreach { log => log.updateRemoteIndexHighestOffset(readOffset) }

    def convertToLeader(leaderEpochVal: Int): Unit = {
      if (leaderEpochVal < 0) {
        throw new KafkaException(s"leaderEpoch value for topic partition $tpId can not be negative")
      }
      if (this.leaderEpoch != leaderEpochVal) {
        leaderEpoch = leaderEpochVal
      }
      // Reset readOffset, so that it is set in next run of RLMTask
      readOffsetOption = None
    }

    def convertToFollower(): Unit = {
      leaderEpoch = -1
    }

    def copyLogSegmentsToRemote(): Unit = {
      if (isCancelled())
        return

      def maybeUpdateReadOffset(): Unit = {
        if (readOffsetOption.isEmpty) {
          info(s"Find the highest remote offset for partition: $tpId after becoming leader, leaderEpoch: $leaderEpoch")

          // This is found by traversing from the latest leader epoch from leader epoch history and find the highest offset
          // of a segment with that epoch copied into remote storage. If it can not find an entry then it checks for the
          // previous leader epoch till it finds an entry, If there are no entries till the earliest leader epoch in leader
          // epoch cache then it starts copying the segments from the earliest epoch entry’s offset.
          readOffsetOption = Some(findHighestRemoteOffset(tpId))
        }
      }

      try {
        maybeUpdateReadOffset()
        val readOffset = readOffsetOption.get
        fetchLog(tpId).foreach { log =>
          // LSO indicates the offset below are ready to be consumed(high-watermark or committed)
          val lso = log.lastStableOffset
          if (lso < 0) {
            warn(s"lastStableOffset for partition $tpId is $lso, which should not be negative.")
          } else if (lso > 0 && readOffset < lso) {
            // copy segments only till the min of high-watermark or stable-offset
            // remote storage should contain only committed/acked messages
            val fetchOffset = lso
            info(s"Checking for segments to copy, readOffset: $readOffset and fetchOffset: $fetchOffset")
            val activeSegBaseOffset = log.activeSegment.baseOffset
            // log-start-offset can be ahead of the read-offset, when:
            // 1) log-start-offset gets incremented via delete-records API (or)
            // 2) enabling the remote log for the first time, the log-start-offset can be ahead of the local-log base-segment-offset due to segment deletion.
            val fromOffset = Math.max(readOffset + 1, log.logStartOffset)
            val sortedSegments = log.logSegments(fromOffset, fetchOffset).toSeq.sortBy(_.baseOffset)
            val index: Int = sortedSegments.map(x => x.baseOffset).search(activeSegBaseOffset) match {
              case Found(x) => x
              case InsertionPoint(y) => y - 1
            }
            if (index < 0) {
              info(s"No segments found to be copied for partition $tpId with read offset: $readOffset and active " +
                s"baseoffset: $activeSegBaseOffset")
            } else {
              sortedSegments.slice(0, index).foreach { segment =>
                // store locally here as this may get updated after the below if condition is computed as false.
                if (isCancelled() || !isLeader()) {
                  info(s"Skipping copying log segments as the current task state is changed, cancelled: " +
                    s"${isCancelled()} leader:${isLeader()}")
                  return
                }

                val logFile = segment.log.file()
                val fileName = logFile.getName
                info(s"Copying $fileName to remote storage.")
                val id = new RemoteLogSegmentId(tpId, Uuid.randomUuid())



                val nextOffset = segment.readNextOffset
                //todo-tier double check on this
                val endOffset = nextOffset - 1
                val producerIdSnapshotFile: File = log.producerStateManager.fetchSnapshot(nextOffset).get().asInstanceOf[File]


                val segmentLeaderEpochs = getLeaderEpochCheckpoint(log, segment.baseOffset, nextOffset).read().asScala.map(
                  entry => {
                    java.lang.Integer.valueOf(entry.epoch) -> java.lang.Long.valueOf(entry.startOffset)
                  }).toMap.asJava




//                  .stream().collect(
//                  Collectors.toMap(epochEntry => epochEntry.epoch, epochEntry => epochEntry.startOffset))


                val remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(id, segment.baseOffset, endOffset,
                  segment.largestTimestamp, brokerId, time.milliseconds(), segment.log.sizeInBytes(),
                  segmentLeaderEpochs)


                remoteLogMetadataManager.addRemoteLogSegmentMetadata(remoteLogSegmentMetadata)

                val leaderEpochsIndex = getLeaderEpochCheckpoint(log, startOffset = -1, nextOffset).readAsByteBuffer()

                val segmentData = new LogSegmentData(logFile.toPath, segment.lazyOffsetIndex.get.file().toPath,
                  segment.lazyTimeIndex.get.file().toPath, Optional.ofNullable(if (segment.txnIndex.file().exists()) segment.txnIndex.file().toPath else null),
                  producerIdSnapshotFile.toPath, leaderEpochsIndex)
                remoteLogStorageManager.copyLogSegmentData(remoteLogSegmentMetadata, segmentData)

                val rlsmAfterCreate = new RemoteLogSegmentMetadataUpdate(id, time.milliseconds(),
                  RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId)

                remoteLogMetadataManager.updateRemoteLogSegmentMetadata(rlsmAfterCreate)

                readOffsetOption = Some(endOffset)
                //todo-tier-storage
                log.updateRemoteIndexHighestOffset(endOffset)
                info(s"Copied $fileName to remote storage with segment-id: ${rlsmAfterCreate.remoteLogSegmentId()}")
              }
            }
          } else {
            info(s"Skipping copying segments, current read offset:$readOffset is and LSO:$lso ")
          }
        }
      } catch {
        case ex: Exception =>
//          brokerTopicStats.topicStats(tpId.topicPartition().topic()).failedRemoteWriteRequestRate.mark()
//          brokerTopicStats.allTopicsStats.failedRemoteWriteRequestRate.mark()
          if (!isCancelled()) {
            error(s"Error occurred while copying log segments of partition: $tpId", ex)
          }
      }
    }

    def handleExpiredRemoteLogSegments(): Unit = {
      info("handleExpiredRemoteLogSegments")
      if (isCancelled())
        return

      def handleLogStartOffsetUpdate(topicPartition: TopicPartition, remoteLogStartOffset: Long): Unit = {
        debug(s"Updating $topicPartition with remoteLogStartOffset: $remoteLogStartOffset")
        updateRemoteLogStartOffset(topicPartition, remoteLogStartOffset)
      }

      try {
        // cleanup remote log segments and update the log start offset if applicable.
        // Compute total size, this can be pushed to RLMM by introducing a new method instead of going through
        // the collection every time.
        val segmentMetadataList = remoteLogMetadataManager.listRemoteLogSegments(tpId).asScala.toSeq
        if (segmentMetadataList.nonEmpty) {

          fetchLog(tpId).foreach { log =>
            val retentionMs = log.config.retentionMs
            val totalSize = log.size + segmentMetadataList.map(_.segmentSizeInBytes()).sum
            val (checkTimestampRetention, cleanupTs) = (retentionMs > -1, time.milliseconds() - retentionMs)
            val checkSizeRetention = log.config.retentionSize > -1
            var remainingSize = totalSize - log.config.retentionSize
            var logStartOffset: Option[Long] = None

            def deleteRetentionTimeBreachedSegments(metadata: RemoteLogSegmentMetadata): Boolean = {
              val isSegmentDeleted = deleteRemoteLogSegment(
                metadata, checkTimestampRetention && _.maxTimestampMs() <= cleanupTs)
              if (isSegmentDeleted) {
                remainingSize = Math.max(0, remainingSize - metadata.segmentSizeInBytes())
                // It is fine to have logStartOffset as `metadata.endOffset() + 1` as the segment offset intervals
                // are ascending with in an epoch.
                logStartOffset = Some(metadata.endOffset() + 1)
                info(s"!!! Deleted remote log segment ${metadata.remoteLogSegmentId()} due to retention time " +
                  s"${retentionMs}ms breach based on the largest record timestamp in the segment")
              }
              isSegmentDeleted
            }

            def deleteRetentionSizeBreachedSegments(metadata: RemoteLogSegmentMetadata): Boolean = {
              val isSegmentDeleted = deleteRemoteLogSegment(metadata, metadata => {
                // Assumption that segments contain size > 0
                if (checkSizeRetention && remainingSize > 0) {
                  remainingSize -= metadata.segmentSizeInBytes()
                  remainingSize >= 0
                } else false
              })
              if (isSegmentDeleted) {
                logStartOffset = Some(metadata.endOffset() + 1)
                info(s"Deleted remote log segment ${metadata.remoteLogSegmentId()} due to retention size " +
                  s"${log.config.retentionSize} breach. Log size after deletion will be " +
                  s"${remainingSize + log.config.retentionSize}.")
              }
              isSegmentDeleted
            }

            log.leaderEpochCache.foreach { cache =>
              cache.epochEntries.asScala.find { epochEntry =>
                val segmentsIterator = remoteLogMetadataManager.listRemoteLogSegments(tpId, epochEntry.epoch)
                var isSegmentDeleted = true
                while (isSegmentDeleted && segmentsIterator.hasNext) {
                  val metadata = segmentsIterator.next()
                  isSegmentDeleted = deleteRetentionTimeBreachedSegments(metadata) ||
                    deleteRetentionSizeBreachedSegments(metadata)
                }
                !isSegmentDeleted
              }
            }
            logStartOffset.foreach(handleLogStartOffsetUpdate(tpId.topicPartition(), _))
          }
        }
      } catch {
        case ex: Exception =>
          if (!isCancelled()) {
            error(s"Error while cleaning up log segments for partition: $tpId", ex)
          }
      }
    }

    override def run(): Unit = {
      if (isCancelled())
        return

      try {
        if (isLeader()) {
          // a. copy log segments to remote store
          copyLogSegmentsToRemote()
          // b. cleanup/delete expired remote segments
          // Followers will cleanup the local log cleanup based on the local logStartOffset.
          // We do not need any cleanup on followers from remote segments perspective.
          handleExpiredRemoteLogSegments()
        } else {
          fetchLog(tpId).foreach { log =>
            val offset = findHighestRemoteOffset(tpId)
            log.updateRemoteIndexHighestOffset(offset)
          }
        }
      } catch {
        case ex: InterruptedException =>
          if (!isCancelled()) {
            warn(s"Current thread for topic-partition-id $tpId is interrupted, this task won't be rescheduled. " +
              s"Reason: ${ex.getMessage}")
          }
        case ex: Exception =>
          if (!isCancelled()) {
            warn(s"Current task for topic-partition $tpId received error but it will be scheduled. " +
              s"Reason: ${ex.getMessage}")
          }
      }
    }

    override def toString: String = {
      this.getClass.toString + s"[$tpId]"
    }
  }

}


class RLMScheduledThreadPool(poolSize: Int) extends Logging {

  private val scheduledThreadPool: ScheduledThreadPoolExecutor = {
    val threadPool = new ScheduledThreadPoolExecutor(poolSize)
    threadPool.setRemoveOnCancelPolicy(true)
    threadPool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    threadPool.setThreadFactory(new ThreadFactory {
      private val sequence = new AtomicInteger()

      override def newThread(r: Runnable): Thread = {
        KafkaThread.daemon("kafka-rlm-thread-pool-" + sequence.incrementAndGet(), r)
      }
    })

    threadPool
  }

  def resizePool(size: Int): Unit = {
    info(s"Resizing pool from ${scheduledThreadPool.getCorePoolSize} to $size")
    scheduledThreadPool.setCorePoolSize(size)
  }

  def poolSize(): Int = scheduledThreadPool.getMaximumPoolSize

  def getIdlePercent(): Double = {
    1 - scheduledThreadPool.getActiveCount().asInstanceOf[Double] / scheduledThreadPool.getCorePoolSize.asInstanceOf[Double]
  }

  def scheduleWithFixedDelay(runnable: Runnable, initialDelay: Long, delay: Long,
                             timeUnit: TimeUnit): ScheduledFuture[_] = {
    info(s"Scheduling runnable $runnable with initial delay: $initialDelay, fixed delay: $delay")
    scheduledThreadPool.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit)
  }

  def shutdown(): Boolean = {
    info("Shutting down scheduled thread pool")
    scheduledThreadPool.shutdownNow()
    //waits for 2 mins to terminate the current tasks
    scheduledThreadPool.awaitTermination(2, TimeUnit.MINUTES)
  }
}

trait CancellableRunnable extends Runnable {
  @volatile private var cancelled = false

  def cancel(): Unit = {
    cancelled = true
  }

  def isCancelled(): Boolean = {
    cancelled
  }
}