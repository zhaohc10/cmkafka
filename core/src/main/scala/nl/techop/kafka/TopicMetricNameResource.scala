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

package nl.techop.kafka

import java.util.Collections
import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}

import kafka.server.KafkaServer

import scala.collection.JavaConverters._

@Path("/topicmetricnames")
@Produces(Array(MediaType.APPLICATION_JSON))
class TopicMetricNameResource(server: KafkaServer) {

  @GET
  def topicNames(): TopicNames = {
    val topicNames = server.zkClient.getAllTopicsInCluster
    val result = topicNames.map { topicName =>
      (topicName, topicNameToMetricId(topicName))
    }.toMap.asJava
    new TopicNames(result)
  }

  def topicNameToMetricId(topicName: String): String = {
    topicName.replace('.', '_')
  }
}

class TopicNames(val topicNames: java.util.Map[String, String]) {
  def this() = this(Collections.emptyMap())
  def getTopicNames(): java.util.Map[String, String] = topicNames
}
