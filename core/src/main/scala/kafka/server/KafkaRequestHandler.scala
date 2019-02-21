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

package kafka.server

import java.util
import java.util.Objects

import kafka.network._
import kafka.utils._
import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import com.github.benmanes.caffeine.cache._
import com.yammer.metrics.core.Meter
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.{KafkaThread, Time}

import scala.collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}

/**
 * A thread that answers kafka requests.
 */
class KafkaRequestHandler(id: Int,
                          brokerId: Int,
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: AtomicInteger,
                          val requestChannel: RequestChannel,
                          apis: KafkaApis,
                          time: Time) extends Runnable with Logging {
  this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "
  private val shutdownComplete = new CountDownLatch(1)
  @volatile private var stopped = false

  def run(): Unit = {
    while (!stopped) {
      // We use a single meter for aggregate idle percentage for the thread pool.
      // Since meter is calculated as total_recorded_value / time_window and
      // time_window is independent of the number of threads, each recorded idle
      // time should be discounted by # threads.
      val startSelectTime = time.nanoseconds

      val req = requestChannel.receiveRequest(300)
      val endTime = time.nanoseconds
      val idleTime = endTime - startSelectTime
      aggregateIdleMeter.mark(idleTime / totalHandlerThreads.get)

      req match {
        case RequestChannel.ShutdownRequest =>
          debug(s"Kafka request handler $id on broker $brokerId received shut down command")
          shutdownComplete.countDown()
          return

        case request: RequestChannel.Request =>
          try {
            request.requestDequeueTimeNanos = endTime
            trace(s"Kafka request handler $id on broker $brokerId handling request $request")
            apis.handle(request)
          } catch {
            case e: FatalExitError =>
              shutdownComplete.countDown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            request.releaseBuffer()
          }

        case null => // continue
      }
    }
    shutdownComplete.countDown()
  }

  def stop(): Unit = {
    stopped = true
  }

  def initiateShutdown(): Unit = requestChannel.sendShutdownRequest()

  def awaitShutdown(): Unit = shutdownComplete.await()

}

class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              time: Time,
                              numThreads: Int,
                              requestHandlerAvgIdleMetricName: String,
                              logAndThreadNamePrefix : String) extends Logging with KafkaMetricsGroup {

  private val threadPoolSize: AtomicInteger = new AtomicInteger(numThreads)
  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = newMeter(requestHandlerAvgIdleMetricName, "percent", TimeUnit.NANOSECONDS)

  this.logIdent = "[" + logAndThreadNamePrefix + " Kafka Request Handler on Broker " + brokerId + "], "
  val runnables = new mutable.ArrayBuffer[KafkaRequestHandler](numThreads)
  for (i <- 0 until numThreads) {
    createHandler(i)
  }

  def createHandler(id: Int): Unit = synchronized {
    runnables += new KafkaRequestHandler(id, brokerId, aggregateIdleMeter, threadPoolSize, requestChannel, apis, time)
    KafkaThread.daemon(logAndThreadNamePrefix + "-kafka-request-handler-" + id, runnables(id)).start()
  }

  def resizeThreadPool(newSize: Int): Unit = synchronized {
    val currentSize = threadPoolSize.get
    info(s"Resizing request handler thread pool size from $currentSize to $newSize")
    if (newSize > currentSize) {
      for (i <- currentSize until newSize) {
        createHandler(i)
      }
    } else if (newSize < currentSize) {
      for (i <- 1 to (currentSize - newSize)) {
        runnables.remove(currentSize - i).stop()
      }
    }
    threadPoolSize.set(newSize)
  }

  def shutdown(): Unit = synchronized {
    info("shutting down")
    for (handler <- runnables)
      handler.initiateShutdown()
    for (handler <- runnables)
      handler.awaitShutdown()
    info("shut down completely")
  }
}

class BrokerTopicMetrics(topicName: Option[String], partition: Option[String] = None) extends KafkaMetricsGroup {
  val tags: scala.collection.Map[String, String] = topicName match {
    case None => Map.empty
    case Some(topic) => {
      if(partition.isEmpty) Map("topic" -> topic)
      else Map("topic" -> topic, "partition" -> partition.get)
    }
  }

  case class MeterWrapper(metricType: String, eventType: String) {
    @volatile private var lazyMeter: Meter = _
    private val meterLock = new Object

    def meter(): Meter = {
      var meter = lazyMeter
      if (meter == null) {
        meterLock synchronized {
          meter = lazyMeter
          if (meter == null) {
            meter = newMeter(metricType, eventType, TimeUnit.SECONDS, tags)
            lazyMeter = meter
          }
        }
      }
      meter
    }

    def close(): Unit = meterLock synchronized {
      if (lazyMeter != null) {
        removeMetric(metricType, tags)
        lazyMeter = null
      }
    }

    if (tags.isEmpty) // greedily initialize the general topic metrics
      meter()
  }

  // an internal map for "lazy initialization" of certain metrics
  private val metricTypeMap = new Pool[String, MeterWrapper]()
  metricTypeMap.putAll(Map(
    BrokerTopicStats.MessagesInPerSec -> MeterWrapper(BrokerTopicStats.MessagesInPerSec, "messages"),
    BrokerTopicStats.BytesInPerSec -> MeterWrapper(BrokerTopicStats.BytesInPerSec, "bytes"),
    BrokerTopicStats.BytesOutPerSec -> MeterWrapper(BrokerTopicStats.BytesOutPerSec, "bytes"),
    BrokerTopicStats.BytesRejectedPerSec -> MeterWrapper(BrokerTopicStats.BytesRejectedPerSec, "bytes"),
    BrokerTopicStats.FailedProduceRequestsPerSec -> MeterWrapper(BrokerTopicStats.FailedProduceRequestsPerSec, "requests"),
    BrokerTopicStats.FailedFetchRequestsPerSec -> MeterWrapper(BrokerTopicStats.FailedFetchRequestsPerSec, "requests"),
    BrokerTopicStats.TotalProduceRequestsPerSec -> MeterWrapper(BrokerTopicStats.TotalProduceRequestsPerSec, "requests"),
    BrokerTopicStats.TotalFetchRequestsPerSec -> MeterWrapper(BrokerTopicStats.TotalFetchRequestsPerSec, "requests"),
    BrokerTopicStats.FetchMessageConversionsPerSec -> MeterWrapper(BrokerTopicStats.FetchMessageConversionsPerSec, "requests"),
    BrokerTopicStats.ProduceMessageConversionsPerSec -> MeterWrapper(BrokerTopicStats.ProduceMessageConversionsPerSec, "requests"),
    BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec -> MeterWrapper(BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec, "requests"),
    BrokerTopicStats.InvalidMagicNumberRecordsPerSec -> MeterWrapper(BrokerTopicStats.InvalidMagicNumberRecordsPerSec, "requests"),
    BrokerTopicStats.InvalidMessageCrcRecordsPerSec -> MeterWrapper(BrokerTopicStats.InvalidMessageCrcRecordsPerSec, "requests"),
    BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec -> MeterWrapper(BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec, "requests")
  ).asJava)
  if (topicName.isEmpty) {
    metricTypeMap.put(BrokerTopicStats.ReplicationBytesInPerSec, MeterWrapper(BrokerTopicStats.ReplicationBytesInPerSec, "bytes"))
    metricTypeMap.put(BrokerTopicStats.ReplicationBytesOutPerSec, MeterWrapper(BrokerTopicStats.ReplicationBytesOutPerSec, "bytes"))
  }

  // used for testing only
  def metricMap: Map[String, MeterWrapper] = metricTypeMap.toMap

  def messagesInRate = metricTypeMap.get(BrokerTopicStats.MessagesInPerSec).meter()

  def bytesInRate = metricTypeMap.get(BrokerTopicStats.BytesInPerSec).meter()

  def bytesOutRate = metricTypeMap.get(BrokerTopicStats.BytesOutPerSec).meter()

  def bytesRejectedRate = metricTypeMap.get(BrokerTopicStats.BytesRejectedPerSec).meter()

  private[server] def replicationBytesInRate =
    if (topicName.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReplicationBytesInPerSec).meter())
    else None

  private[server] def replicationBytesOutRate =
    if (topicName.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReplicationBytesOutPerSec).meter())

//  val messagesInRate = newMeter(BrokerTopicStats.MessagesInPerSec, "messages", TimeUnit.SECONDS, tags)
//  val bytesInRate = newMeter(BrokerTopicStats.BytesInPerSec, "bytes", TimeUnit.SECONDS, tags)
//  val bytesOutRate = newMeter(BrokerTopicStats.BytesOutPerSec, "bytes", TimeUnit.SECONDS, tags)
//  val bytesRejectedRate = newMeter(BrokerTopicStats.BytesRejectedPerSec, "bytes", TimeUnit.SECONDS, tags)
//  private[server] val replicationBytesInRate =
//    if (topicName.isEmpty) Some(newMeter(BrokerTopicStats.ReplicationBytesInPerSec, "bytes", TimeUnit.SECONDS, tags))
//    else None
//  private[server] val replicationBytesOutRate =
//    if (topicName.isEmpty) Some(newMeter(BrokerTopicStats.ReplicationBytesOutPerSec, "bytes", TimeUnit.SECONDS, tags))

    else None

  def failedProduceRequestRate = metricTypeMap.get(BrokerTopicStats.FailedProduceRequestsPerSec).meter()

  def failedFetchRequestRate = metricTypeMap.get(BrokerTopicStats.FailedFetchRequestsPerSec).meter()

  def totalProduceRequestRate = metricTypeMap.get(BrokerTopicStats.TotalProduceRequestsPerSec).meter()

  def totalFetchRequestRate = metricTypeMap.get(BrokerTopicStats.TotalFetchRequestsPerSec).meter()

  def fetchMessageConversionsRate = metricTypeMap.get(BrokerTopicStats.FetchMessageConversionsPerSec).meter()

  def produceMessageConversionsRate = metricTypeMap.get(BrokerTopicStats.ProduceMessageConversionsPerSec).meter()

  def noKeyCompactedTopicRecordsPerSec = metricTypeMap.get(BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec).meter()

  def invalidMagicNumberRecordsPerSec = metricTypeMap.get(BrokerTopicStats.InvalidMagicNumberRecordsPerSec).meter()

  def invalidMessageCrcRecordsPerSec = metricTypeMap.get(BrokerTopicStats.InvalidMessageCrcRecordsPerSec).meter()

  def invalidOffsetOrSequenceRecordsPerSec = metricTypeMap.get(BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec).meter()

  def closeMetric(metricType: String): Unit = {
    val meter = metricTypeMap.get(metricType)
    if (meter != null)
      meter.close()
  }

  def close(): Unit = metricTypeMap.values.foreach(_.close())
}

object BrokerTopicStats {
  val MessagesInPerSec = "MessagesInPerSec"
  val BytesInPerSec = "BytesInPerSec"
  val BytesOutPerSec = "BytesOutPerSec"
  val BytesRejectedPerSec = "BytesRejectedPerSec"
  val ReplicationBytesInPerSec = "ReplicationBytesInPerSec"
  val ReplicationBytesOutPerSec = "ReplicationBytesOutPerSec"
  val FailedProduceRequestsPerSec = "FailedProduceRequestsPerSec"
  val FailedFetchRequestsPerSec = "FailedFetchRequestsPerSec"
  val TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec"
  val TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec"
  val FetchMessageConversionsPerSec = "FetchMessageConversionsPerSec"
  val ProduceMessageConversionsPerSec = "ProduceMessageConversionsPerSec"

  // These following topics are for LogValidator for better debugging on failed records
  val NoKeyCompactedTopicRecordsPerSec = "NoKeyCompactedTopicRecordsPerSec"
  val InvalidMagicNumberRecordsPerSec = "InvalidMagicNumberRecordsPerSec"
  val InvalidMessageCrcRecordsPerSec = "InvalidMessageCrcRecordsPerSec"
  val InvalidOffsetOrSequenceRecordsPerSec = "InvalidOffsetOrSequenceRecordsPerSec"

  private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k))
  private val topicPartitionValueFactory = (k: (String, Integer)) => new BrokerTopicMetrics(Some(k._1), Some(k._2.toString))
}

class ProducerStats(producerCacheMaxSize: Int, producerCacheExpiryMs: Long) extends Logging {
  Objects.requireNonNull(producerCacheMaxSize, "producerCacheMaxSize can not be null")
  Objects.requireNonNull(producerCacheExpiryMs, "producerCacheExpiryMs can not be null")

  private val factory = (k: (String, TopicPartition)) => new BrokerClientMetrics(k._1, k._2)
  private val clientMetrics = new Pool[(String, TopicPartition), BrokerClientMetrics](Some(factory))

  private val removalListener = new RemovalListener[String, util.Collection[TopicPartition]] {
    override def onRemoval(key: String, value: util.Collection[TopicPartition], cause: RemovalCause): Unit = {
      debug(s"Cache removal listener invoked for key: $key and value as $value")
      removeMetrics(key, value)
    }
  }

  private val cacheLoader = new CacheLoader[String, util.Collection[TopicPartition]]() {
    override def load(key: String): util.Collection[TopicPartition] = ConcurrentHashMap.newKeySet()
  }

  private val clientTopicPartitions: LoadingCache[String, util.Collection[TopicPartition]] = Caffeine.newBuilder()
    .expireAfterWrite(producerCacheExpiryMs, TimeUnit.MILLISECONDS)
    .expireAfterAccess(producerCacheExpiryMs, TimeUnit.MILLISECONDS)
    .maximumSize(producerCacheMaxSize)
    .initialCapacity(producerCacheMaxSize/2)
    .removalListener(removalListener)
    .build(cacheLoader)

  // clears cache every 5 mins as caffeine cache does not guarantee to remove entries as soon as expired.
  // This gives deterministic behavior about producers considered to be inactive as those metrics are removed.
  Executors.newScheduledThreadPool(1, new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = Executors.defaultThreadFactory().newThread(r)
      thread.setName("client-topic-metrics-cache-cleanup-thread")
      thread.setDaemon(true)
      thread
    }
  }).scheduleWithFixedDelay(new Runnable {
    override def run(): Unit = {
      clientTopicPartitions.cleanUp()
    }
  }, 5, 300, TimeUnit.SECONDS)

  def clientMetrics(clientId:String, topicPartition: TopicPartition): BrokerClientMetrics = {
    Objects.requireNonNull(topicPartition, "topicPartition can not be null")
    val topicPartitions = clientTopicPartitions.get(clientId)
    if(topicPartitions != null) topicPartitions.add(topicPartition)
    // setting the value as get may invalidate before returning the value.
    clientTopicPartitions.put(clientId, topicPartitions)

    clientMetrics.getAndMaybePut(Tuple2(clientId, topicPartition))
  }

  def removeMetrics(clientId: String) {
    val partitions: util.Collection[TopicPartition] = clientTopicPartitions.getIfPresent(clientId)
    removeMetrics(clientId, partitions)
  }

  private def removeMetrics(clientId: String, partitions: util.Collection[TopicPartition]) = {
    debug(s"Removing clientId: $clientId with partitions: $partitions")
    if (partitions != null && !partitions.isEmpty) {
      JavaConversions.collectionAsScalaIterable(partitions).foreach(tp => {
        val metrics = clientMetrics.remove((clientId, tp))
        if (metrics != null) metrics.close()
      })
    }
  }

  def close(): Unit = {
    clientMetrics.values.foreach(_.close())
  }
}

class BrokerClientMetrics(clientId:String, topicPartition: TopicPartition) extends Logging with KafkaMetricsGroup {
  val tags: scala.collection.Map[String, String] =
    Map("clientId"-> clientId, "topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString)

  val messagesInRate = newMeter(BrokerTopicStats.MessagesInPerSec, "messages", TimeUnit.SECONDS, tags)

  def close(): Unit = {
    removeMetric(BrokerTopicStats.MessagesInPerSec, tags)
    debug(s"Removing client metrics $tags")
  }
}

class BrokerTopicStats {
  import BrokerTopicStats._

  private val topicStats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  private val topicPartitionStats = new Pool[(String, Integer), BrokerTopicMetrics](Some(topicPartitionValueFactory))

  val allTopicsStats = new BrokerTopicMetrics(None)

  def topicStats(topic: String, partition: Integer = null): BrokerTopicMetrics = {
    if(partition != null)
      topicPartitionStats.getAndMaybePut((topic, partition))
    else
      topicStats.getAndMaybePut(topic)
  }

  def updateReplicationBytesIn(value: Long): Unit = {
    allTopicsStats.replicationBytesInRate.foreach { metric =>
      metric.mark(value)
    }
  }

  private def updateReplicationBytesOut(value: Long): Unit = {
    allTopicsStats.replicationBytesOutRate.foreach { metric =>
      metric.mark(value)
    }
  }

  // This method only removes metrics only used for leader
  def removeOldLeaderMetrics(topic: String): Unit = {
    val topicMetrics = topicStats(topic)
    if (topicMetrics != null) {
      topicMetrics.closeMetric(BrokerTopicStats.MessagesInPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.BytesInPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.BytesRejectedPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.FailedProduceRequestsPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.TotalProduceRequestsPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ProduceMessageConversionsPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ReplicationBytesOutPerSec)
    }
  }

  // This method only removes metrics only used for follower
  def removeOldFollowerMetrics(topic: String): Unit = {
    val topicMetrics = topicStats(topic)
    if (topicMetrics != null)
      topicMetrics.closeMetric(BrokerTopicStats.ReplicationBytesInPerSec)
  }


  def removeMetrics(topic: String) {
    val metrics = topicStats.remove(topic)
    if (metrics != null)
      metrics.close()
  }

  def removeMetrics(topicPartition: TopicPartition) {
    val partitionMetrics = topicPartitionStats.remove(Tuple2(topicPartition.topic, topicPartition.partition))
    if(partitionMetrics != null) partitionMetrics.close()
  }

//  def updateBytesOut(topic: String, isFollower: Boolean, value: Long): Unit = {
  def updateBytesOut(topicPartition: TopicPartition, isFollower: Boolean, value: Long) {
    if (isFollower) {
      updateReplicationBytesOut(value)
    } else {
      topicStats(topicPartition.topic()).bytesOutRate.mark(value)
      topicStats(topicPartition.topic(), topicPartition.partition).bytesOutRate.mark(value)
      allTopicsStats.bytesOutRate.mark(value)
    }
  }

  def close(): Unit = {
    allTopicsStats.close()
    topicStats.values.foreach(_.close())
  }

}
