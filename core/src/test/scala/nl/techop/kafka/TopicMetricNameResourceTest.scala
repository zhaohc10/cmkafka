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

import java.util.Properties
import javax.ws.rs.client.ClientBuilder

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider
import kafka.integration.KafkaServerTestHarness
import kafka.metrics.{KafkaMetricsReporter, KafkaMetricsReporterMBean, KafkaServerMetricsReporter}
import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, VerifiableProperties}
import org.glassfish.jersey.client.ClientConfig
import org.junit.{After, Before, Ignore, Test}
import org.junit.Assert._

import scala.util.{Failure, Success, Try}

class TopicMetricNameResourceTest extends KafkaServerTestHarness {

  var reporters: scala.Seq[KafkaMetricsReporter] = null
  var props: Properties = null

  override def generateConfigs: Seq[KafkaConfig] = {
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.setProperty("kafka.metrics.reporters", "nl.techop.kafka.KafkaHttpMetricsReporter")
    this.props = props
    Seq(KafkaConfig.fromProps(props))
  }

  private def shutDownReporters(reporters: scala.Seq[KafkaMetricsReporter]): Unit = {
    reporters.filter(_.isInstanceOf[KafkaMetricsReporterMBean])
      .foreach(_.asInstanceOf[KafkaMetricsReporterMBean].stopReporter())
  }

  @Before
  override def setUp(): Unit = {
    super.setUp()
    var hasPortAssigned = false
    var port = KafkaHttpMetricsReporter.defaultPort
    while (!hasPortAssigned && port <= 9090) {
      props.setProperty("kafka.http.metrics.port", port.toString)
      val reportersStartup = Try {
        reporters = KafkaMetricsReporter.startReporters(new VerifiableProperties(props))
        reporters.filter(_.isInstanceOf[KafkaServerMetricsReporter]).
          foreach(_.asInstanceOf[KafkaServerMetricsReporter].setServer(servers.head))
      }
      reportersStartup match {
        case Success(_) => hasPortAssigned = true
        case Failure(_) => {
          port = port + 1
          shutDownReporters(reporters)
        }
      }
    }
  }

  @After
  override def tearDown(): Unit = {
    shutDownReporters(reporters)
    super.tearDown()
  }

  @Ignore
  @Test
  def testTopicMetricNamesEndpoint(): Unit = {
    TestUtils.createTopic(zkClient, "tmnr.test", 1, 1, servers, new Properties())
    val config = new ClientConfig()
    config.register(classOf[JacksonJsonProvider])
    val client = ClientBuilder.newClient(config)
    val target = client.target("http://localhost:8080")
    val entity = target.path("api").path("topicmetricnames").request().get(classOf[TopicNames])
    assertEquals(entity.getTopicNames().get("tmnr.test"), "tmnr_test")
  }
}
