/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.util.Properties

import kafka.consumer.BaseConsumerRecord
import kafka.tools.MirrorMaker.MirrorMakerProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.record.{RecordBatch, TimestampType}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.sys.process.Process

class MirrorMakerTest {

  @Test
  def testProducerSslPasswordGenerated(): Unit = {
    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093")
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    producerProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG + ".generator", "printf cloudera")
    val producer = new MirrorMakerProducer(true, producerProps)

    assertEquals("cloudera", producer.producerProps.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG))
  }

  @Test
  def testDefaultMirrorMakerMessageHandler() {
    val now = 12345L
    val consumerRecord = BaseConsumerRecord("topic", 0, 1L, now, TimestampType.CREATE_TIME, "key".getBytes, "value".getBytes)

    val result = MirrorMaker.defaultMirrorMakerMessageHandler.handle(consumerRecord)
    assertEquals(1, result.size)

    val producerRecord = result.get(0)
    assertEquals(now, producerRecord.timestamp)
    assertEquals("topic", producerRecord.topic)
    assertNull(producerRecord.partition)
    assertEquals("key", new String(producerRecord.key))
    assertEquals("value", new String(producerRecord.value))
  }

  @Test
  def testDefaultMirrorMakerMessageHandlerWithNoTimestampInSourceMessage(): Unit = {
    val consumerRecord = BaseConsumerRecord("topic", 0, 1L, RecordBatch.NO_TIMESTAMP, TimestampType.CREATE_TIME,
      "key".getBytes, "value".getBytes)

    val result = MirrorMaker.defaultMirrorMakerMessageHandler.handle(consumerRecord)
    assertEquals(1, result.size)

    val producerRecord = result.get(0)
    assertNull(producerRecord.timestamp)
    assertEquals("topic", producerRecord.topic)
    assertNull(producerRecord.partition)
    assertEquals("key", new String(producerRecord.key))
    assertEquals("value", new String(producerRecord.value))
  }

  @Test
  def testDefaultMirrorMakerMessageHandlerWithHeaders(): Unit = {
    val now = 12345L
    val consumerRecord = BaseConsumerRecord("topic", 0, 1L, now, TimestampType.CREATE_TIME, "key".getBytes,
      "value".getBytes)
    consumerRecord.headers.add("headerKey", "headerValue".getBytes)
    val result = MirrorMaker.defaultMirrorMakerMessageHandler.handle(consumerRecord)
    assertEquals(1, result.size)

    val producerRecord = result.get(0)
    assertEquals(now, producerRecord.timestamp)
    assertEquals("topic", producerRecord.topic)
    assertNull(producerRecord.partition)
    assertEquals("key", new String(producerRecord.key))
    assertEquals("value", new String(producerRecord.value))
    assertEquals("headerValue", new String(producerRecord.headers.lastHeader("headerKey").value))
    assertEquals(1, producerRecord.headers.asScala.size)
  }
}
