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

package com.cloudera.kafka.wrap

import java.util
import java.util.Properties

import kafka.utils.Logging
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.utils.Exit

import scala.collection.JavaConversions._
import scala.sys.process.Process

object Kafka extends Logging {

  val SslPasswordParams: Array[String] = Array(
    SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
    SslConfigs.SSL_KEY_PASSWORD_CONFIG,
    SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)

  def exec(command: String): String = {
    // removing extra newline character from the end (Process execution puts it there)
    Process(command).!!.replaceAll("(\r\n|\n)$", "")
  }

  def generatePasswordsOverrides(serverProps: Properties): Array[String] = {
    val generatedProps: Properties = generateSslPasswords(serverProps)
    generatedProps.asInstanceOf[util.Hashtable[Object, Object]].putAll(generateDelegationTokenPassword(serverProps))
    val props = propertiesAsScalaMap(generatedProps)

    props.flatMap(k => {
      Some(s"--override ${k._1}=${k._2}")
    }).toArray
  }

  def generateDelegationTokenPassword(props: Properties) : Properties ={
    val generatedProps: Properties = new Properties()
    val masterKeyKey : String = kafka.server.KafkaConfig.DelegationTokenMasterKeyProp
    val enabledKey : String = "delegation.token.enable"
    if ("true" == props.getProperty(enabledKey)) {
      generatePasswordForKey(masterKeyKey, props, generatedProps)
    }
    props.remove(generatorKeyFor(masterKeyKey))
    props.remove(enabledKey)
    generatedProps
  }

  def generateSslPasswords(props: Properties): Properties = {
    val generatedProps: Properties = new Properties()
    SslPasswordParams.foreach(key => generatePasswordForKey(key, props, generatedProps))
    generatedProps
  }

  def generatePasswordForKey(key: String, props: Properties, generatedProps: Properties) : Unit = {
    val generatorKey: String = generatorKeyFor(key)
    val value = props.getProperty(generatorKey)
    if (value != null) {
      try {
        props.remove(generatorKey)
        generatedProps.put(key, exec(value))
        debug(s"Generated password for $key")
      } catch {
        case e: Exception =>
          error(s"Failed to generate password for $key.\n$e")

      }
    }

  }

  def generatorKeyFor(key: String) = {
    key + ".generator"
  }

  def main(args: Array[String]): Unit = {
    val mainMethod : Array[String] => Unit = kafka.Kafka.main
    val serverProps = kafka.Kafka.getPropsFromArgs(args)

    runMain(args, serverProps, mainMethod)
  }

  def runMain(args: Array[String], serverProps: Properties, mainMethod: Array[String] => Unit): Any = {
    try {
      val passwordsOverrides = generatePasswordsOverrides(serverProps)
      val argsWithOverrides: Array[String] = args ++ passwordsOverrides.flatMap(_.split(" "))

      mainMethod(argsWithOverrides)
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception ", e)
        Exit.exit(1)
    }
    Exit.exit(0)
  }
}
