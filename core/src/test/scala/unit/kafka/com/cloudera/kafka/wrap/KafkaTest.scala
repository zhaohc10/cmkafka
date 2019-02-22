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

package kafka.com.cloudera.kafka.wrap

import java.util.Properties

import com.cloudera.kafka.wrap.Kafka
import com.cloudera.kafka.wrap.Kafka.generatorKeyFor
import kafka.server.KafkaConfig.DelegationTokenMasterKeyProp
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.utils.Exit.Procedure
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

import scala.collection.JavaConversions._


class KafkaTest {

  @Test
  def testGeneratePasswordForKey(): Unit = {
    val key = "test.key"
    val originalProps = new Properties
    originalProps.put("test.key.generator", "echo P4S5W0RD-1")
    val generatedProps = new Properties
    Kafka.generatePasswordForKey(key, originalProps, generatedProps)
    assertEquals("P4S5W0RD-1", generatedProps.getProperty(key).trim)
    assertFalse(originalProps.containsKey("test.key.generator"))
  }

  @Test
  def testGenerateDelegationTokenPasswordDisabled(): Unit = {
    val originalProps = new Properties
    originalProps.put("delegation.token.enable", "false")
    originalProps.put(generatorKeyFor(DelegationTokenMasterKeyProp), "echo M4573RK3Y")
    val generated = Kafka.generateDelegationTokenPassword(originalProps)
    assertFalse(originalProps.containsKey("delegation.token.enable"))
    assertFalse(generated.containsKey(DelegationTokenMasterKeyProp))
  }

  @Test
  def testGenerateDelegationTokenPasswordEnabled(): Unit = {
    val originalProps = new Properties
    originalProps.put("delegation.token.enable", "true")
    originalProps.put(generatorKeyFor(DelegationTokenMasterKeyProp), "echo M4573RK3Y")
    val generated = Kafka.generateDelegationTokenPassword(originalProps)
    assertFalse(originalProps.containsKey("delegation.token.enable"))
    assertEquals("M4573RK3Y", generated.getProperty(DelegationTokenMasterKeyProp).trim )
  }

  @Test
  def testGenerateSslConfigPasswords(): Unit = {
    val originalProps = new Properties
    originalProps.put("delegation.token.enable", "true")
    originalProps.put(generatorKeyFor(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), "echo p1")
    originalProps.put(generatorKeyFor(SslConfigs.SSL_KEY_PASSWORD_CONFIG), "echo p2")
    originalProps.put(generatorKeyFor(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG), "echo p3")

    val generated = Kafka.generateSslPasswords(originalProps)

    assertEquals("p1", generated.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).trim)
    assertFalse(originalProps.contains(generatorKeyFor(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)))
    assertEquals("p2", generated.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG).trim)
    assertFalse(originalProps.contains(generatorKeyFor(SslConfigs.SSL_KEY_PASSWORD_CONFIG)))
    assertEquals("p3", generated.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).trim)
    assertFalse(originalProps.contains(generatorKeyFor(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)))
  }

  @Test
  def testGenerateSSLConfigPasswordsUnset(): Unit = {
    val originalProps = new Properties
    originalProps.put("delegation.token.enable", "true")
    originalProps.put(generatorKeyFor(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), "echo p1")
    originalProps.put(generatorKeyFor(SslConfigs.SSL_KEY_PASSWORD_CONFIG), "echo p3")
    originalProps.put(generatorKeyFor(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG), "echo p2")

    val generated = Kafka.generateSslPasswords(originalProps)

    assertFalse(generated.contains(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG))
    assertFalse(generated.contains(SslConfigs.SSL_KEY_PASSWORD_CONFIG))
    assertFalse(generated.contains(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG))
  }


  @Test
  def testRunMain(): Unit =  try {

    val expectedProps = new Properties
    expectedProps.put(DelegationTokenMasterKeyProp, "k1")
    expectedProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "p1")
    expectedProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "p3")
    expectedProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "p2")

    val props = new Properties
    props.put("delegation.token.enable", "true")
    expectedProps.foreach(k => {
      val key = k._1
      props.put(generatorKeyFor(key), "echo " + k._2)
    })


    var mainCalled = false
    Exit.setExitProcedure(new Procedure {
      def execute(status: Int, message: String) = assertEquals(0, status)
    })

    Kafka.runMain(Array("arg1", "arg2"), props, array => {
      mainCalled = true
      assertEquals(("arg1", "arg2"), (array(0), array(1)))
      List(2, 4, 6, 8).foreach(index => {
        assertEquals("--override", array(index))
      })
      assertEquals(
        Set(3, 5, 7, 9).map(array(_).trim),
        expectedProps.map(k=> s"${k._1}=${k._2}").toSet
      )

    })
    assertTrue(mainCalled)
  } finally {
    Exit.resetExitProcedure()
  }

}
