/*
 * Copyright 2016, arnobroekhof@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.techop.kafka

import java.net.InetSocketAddress
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider
import com.yammer.metrics.reporting.{HealthCheckServlet, MetricsServlet, PingServlet, ThreadDumpServlet}
import kafka.metrics.{KafkaMetricsConfig, KafkaMetricsReporterMBean, KafkaServerMetricsReporter}
import kafka.server.KafkaServer
import kafka.utils.{Logging, VerifiableProperties}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

private trait KafkaHttpMetricsReporterMBean extends KafkaMetricsReporterMBean

object KafkaHttpMetricsReporter {
  val defaultPort = 8080
  val defaultBindAddress = "localhost"
}

private class KafkaHttpMetricsReporter extends KafkaServerMetricsReporter
                              with KafkaHttpMetricsReporterMBean
                              with Logging {

  private var metricsServer: Server = null

  private var initialized = false
  private var configured = false
  private var running = false

  private var metricsConfig: KafkaMetricsConfig = null
  private var bindAddress: String = null
  private var port: Int = 0

  override def getMBeanName = "kafka:type=nl.techop.kafka.KafkaHttpMetricsReporter"

  override def init(props: VerifiableProperties): Unit = {
    synchronized {
      if (!initialized) {
        info("Initializing Kafka Http Metrics Reporter")
        metricsConfig = new KafkaMetricsConfig(props)
        bindAddress = props.getString("kafka.http.metrics.host", KafkaHttpMetricsReporter.defaultBindAddress)
        port = props.getInt("kafka.http.metrics.port", KafkaHttpMetricsReporter.defaultPort)

        initialized = true
        info("Initialized Kafka Http Metrics Reporter")
      } else {
        error("Kafka Http Metrics Reporter is already initialized")
      }
    }
  }

  override def setServer(server: KafkaServer) {
    synchronized {
      if (initialized) {
        if (!configured) {
          info("Starting Kafka Http Metrics Reporter")
          // creating the socket address for binding to the specified address and port
          val inetSocketAddress = new InetSocketAddress(bindAddress, port)

          // create new Jetty server
          metricsServer = new Server(inetSocketAddress)

          // creating the servlet context handler
          val handler = new ServletContextHandler(metricsServer, "/*")

          // Add a default 404 Servlet
          addMetricsServlet(handler, new DefaultServlet() with NoDoTrace, "/")

          // Add Metrics Servlets
          addMetricsServlet(handler, new MetricsServlet() with NoDoTrace, "/api/metrics")
          addMetricsServlet(handler, new ThreadDumpServlet() with NoDoTrace, "/api/threads")
          addMetricsServlet(handler, new PingServlet() with NoDoTrace, "/api/ping")
          addMetricsServlet(handler, new HealthCheckServlet() with NoDoTrace, "/api/healthcheck")

          // Add Custom Servlets
          val resourceConfig: ResourceConfig = new ResourceConfig
          resourceConfig.register(new JacksonJsonProvider(), 0)
          resourceConfig.register(new KafkaTopicsResource(server), 0)
          resourceConfig.register(new TopicMetricNameResource(server), 0)

          val servletContainer: ServletContainer = new ServletContainer(resourceConfig)
          val servletHolder: ServletHolder = new ServletHolder(servletContainer)
          handler.addServlet(servletHolder, "/api/*")

          // Add the handler to the server
          metricsServer.setHandler(handler)

          configured = true
          startReporter(metricsConfig.pollingIntervalSecs)
          info("Started Kafka Http Metrics Reporter")
        } else {
          error("Kafka Http Metrics Reporter is already configured")
        }
      } else {
        error("Kafka Http Metrics Reporter is not initialized")
      }
    }
  }

  private def addMetricsServlet(context: ServletContextHandler, servlet: HttpServlet, urlPattern: String): Unit = {
    context.addServlet(new ServletHolder(servlet), urlPattern)
  }

  private trait NoDoTrace extends HttpServlet {
    override def doTrace(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
      resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
    }
  }

  override def startReporter(pollingPeriodSecs: Long) {
    synchronized {
      if (initialized && configured) {
        if (!running) {
          metricsServer.start()
          running = true
          info(s"Started Kafka HTTP metrics reporter at ${metricsServer.getURI}")
        } else {
          error("Kafka Http Metrics Reporter is already running")
        }
      } else {
        error("Kafka Http Metrics Reporter is not initialized or not configured")
      }
    }
  }

  override def stopReporter() {
    synchronized {
      if (initialized && configured) {
        if (running) {
          metricsServer.stop()
          running = false
          info("Stopped Kafka CSV metrics reporter")
        } else {
          error("Kafka Http Metrics Reporter already stopped")
        }
      } else {
        error("Kafka Http Metrics Reporter is not initialized or not configured")
      }
    }
  }

}
