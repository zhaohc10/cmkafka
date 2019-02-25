#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CONFIG_OPTION="--config"
KAFKA_CONF_DIR=${KAFKA_CONF_DIR:-/etc/kafka/conf}
SENTRY_CONF_DIR=${SENTRY_CONF_DIR:-$KAFKA_CONF_DIR/sentry-conf}
SENTRY_SITE_XML=${SENTRY_SITE_XML:-sentry-site.xml}
SENTRY_CONF_FILE=$SENTRY_CONF_DIR/$SENTRY_SITE_XML
USAGE_STRING="USAGE: kafka-sentry [$CONFIG_OPTION <path_to_sentry_conf_dir>] <sentry-cli-arguments>"

if [[ "$CONFIG_OPTION" == $1 ]]; then
  conf_dir=$2
  shift;shift
  conf_file=$conf_dir/$SENTRY_SITE_XML
  if [[ ! -f $conf_file ]]; then
    echo "Configuration file, ${conf_file}, does not exist."
    echo "${USAGE_STRING}"
    exit 1
  fi
else
  if [[ -f "${SENTRY_CONF_FILE}" ]]; then
    conf_file=${SENTRY_CONF_FILE}
  else
    echo "No configuration directory for Sentry specified and default conf file ${SENTRY_CONF_FILE} does not exist. Please provide a configuration directory that contains sentry-site.xml with information on how to connect with Sentry service."
    echo "${USAGE_STRING}"
    exit 1
  fi
fi

# supress the HADOOP_HOME warnings in 1.x.x
export HADOOP_HOME_WARN_SUPPRESS=true

exec $(dirname $0)/kafka-run-class.sh org.apache.sentry.provider.db.generic.tools.SentryShellKafka -conf $conf_file "$@"
