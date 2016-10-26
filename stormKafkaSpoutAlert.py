#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
# Alert for Storm kafka Spout
#
# Alex Bush <abush@hortonworks.com>
#

import time
import urllib2
import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set.
import logging
import traceback
import subprocess
import datetime
import os.path
import re

ZK_SPOUT_PATHS_KEY = 'zk_spout_paths'
ZK_KEY = 'zk_quorum'
BROKER_KEY = 'broker_list'

LAG_TOLERANCE_KEY = 'lag_tolerance'
PREPEND_KAFKA_LIB = 'kafka_lib'

KERBEROS_KEYTAB = '{{cluster-env/smokeuser_keytab}}'
KERBEROS_PRINCIPAL = '{{cluster-env/smokeuser_principal_name}}'
SECURITY_ENABLED_KEY = '{{cluster-env/security_enabled}}'
SMOKEUSER_KEY = "{{cluster-env/smokeuser}}"
EXECUTABLE_SEARCH_PATHS = '{{kerberos-env/executable_search_paths}}'

CONNECTION_TIMEOUT_KEY = 'connection.timeout'
CONNECTION_TIMEOUT_DEFAULT = 5.0

logger = logging.getLogger('ambari_alerts')

def get_tokens():
  """
  Returns a tuple of tokens in the format {{site/property}} that will be used
  to build the dictionary passed into execute
  """
  return ( EXECUTABLE_SEARCH_PATHS, KERBEROS_KEYTAB, KERBEROS_PRINCIPAL, SECURITY_ENABLED_KEY, SMOKEUSER_KEY)

def execute(configurations={}, parameters={}, host_name=None):
  """
  Returns a tuple containing the result code and a pre-formatted result label
  Keyword arguments:
  configurations (dictionary): a mapping of configuration key to value
  parameters (dictionary): a mapping of script parameter key to value
  host_name (string): the name of this host where the alert is running
  """

  if configurations is None:
    return (('UNKNOWN', ['There were no configurations supplied to the script.']))

  # Set configuration settings

  if SMOKEUSER_KEY in configurations:
    smokeuser = configurations[SMOKEUSER_KEY]

  executable_paths = None
  if EXECUTABLE_SEARCH_PATHS in configurations:
    executable_paths = configurations[EXECUTABLE_SEARCH_PATHS]

  security_enabled = False
  if SECURITY_ENABLED_KEY in configurations:
    security_enabled = str(configurations[SECURITY_ENABLED_KEY]).upper() == 'TRUE'

  kerberos_keytab = None
  if KERBEROS_KEYTAB in configurations:
    kerberos_keytab = configurations[KERBEROS_KEYTAB]

  kerberos_principal = None
  if KERBEROS_PRINCIPAL in configurations:
    kerberos_principal = configurations[KERBEROS_PRINCIPAL]
    kerberos_principal = kerberos_principal.replace('_HOST', host_name)

  # parse script arguments
  connection_timeout = CONNECTION_TIMEOUT_DEFAULT
  if CONNECTION_TIMEOUT_KEY in parameters:
    connection_timeout = float(parameters[CONNECTION_TIMEOUT_KEY])

  if ZK_SPOUT_PATHS_KEY in parameters:
    zk_paths = parameters[ZK_SPOUT_PATHS_KEY].split(',')

  if ZK_KEY in parameters:
    zk_quorum = parameters[ZK_KEY]

  if BROKER_KEY in parameters:
    broker_quorum = parameters[BROKER_KEY]

  if LAG_TOLERANCE_KEY in parameters:
    lag_tolerance = parameters[LAG_TOLERANCE_KEY]

  if PREPEND_KAFKA_LIB in parameters:
    kafka_lib = parameters[PREPEND_KAFKA_LIB]
  else:
    kafka_lib = None


    # Set up base command for kerberos and fixed lib
  if kafka_lib:
    lib_path = "/var/lib/ambari-agent/cache/host_scripts/"+kafka_lib
    if not os.path.isfile(lib_path):
      raise Exception("Alert is configured to prepend lib to classpath, however jar not found: "+lib_path)
    base_command = "export CLASSPATH=\":"+lib_path+"\"; "
  else:
    base_command = ""
  #Kerberos for Kafka
  if security_enabled:
    base_command+="export KAFKA_CLIENT_KERBEROS_PARAMS=\"-Djava.security.auth.login.config=/etc/kafka/conf/kafka_client_jaas.conf\"; "
    base_command+='kinit -kt '+kerberos_keytab+' '+kerberos_principal+'; '

  #Loop over paths
  exceptions = list()
  passing = list()
  for zk_path in zk_paths:
    #Loop over paths
    try:
      command = base_command + "/usr/hdp/current/kafka-broker/bin/zookeeper-shell.sh "+zk_quorum+" ls "+zk_path
      output = run_command(command)

      #partition pattern
      pattern = "[\[ ]partition_([0-9]+)[,\]]"
      partitions = re.findall(pattern,output)
      if not partitions:
        raise Exception("No partition information found in Zookeeper at: "+zk_path)
      # Get ZK record for each partition
      for partition in partitions:
        command = base_command + "/usr/hdp/current/kafka-broker/bin/zookeeper-shell.sh "+zk_quorum+" get "+zk_path+"/partition_"+str(partition)
        output = run_command(command)
        pattern = "\{.*\}"
        match = re.search(pattern,output)
        if not match:
          raise Exception("Could not find valid json spout info at: "+zk_path+"/partition_"+str(partition)+" Contents: "+output)
        spout_json = json.loads(match.group(0))
        #Get the actual topic offset
        command = base_command + "/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell -topic "+spout_json["topic"]+" --broker-list "+broker_quorum+" --time -1 --partition "+str(partition)
        if security_enabled:
          command+=" --security-protocol PLAINTEXTSASL"
        output = run_command(command)
        pattern = spout_json["topic"]+":"+str(partition)+":([0-9]+)"
        offset_s = re.search(pattern,output)
        if not offset_s:
          raise Exception("Could not find offset on broker for: "+spout_json["topic"]+" Partition: "+str(partition)+" Contents: "+output)
        offset = int(offset_s.group(1))
        if (int(offset) > (int(spout_json["offset"])+int(lag_tolerance))):
          msg = "Spout lag for spout: %s, on topic: %s, on partition %s exceeded threshold. Current topic offset it: %s, Spout offset is: %s, lag tolerance is: %s" % (spout_json["topology"]["id"],spout_json["topic"],str(partition),offset,spout_json["offset"],lag_tolerance)
          exceptions.append(msg)
        else:
          msg = "Spout lag for spout: %s, on topic: %s, on partition %s within threshold. Current topic offset it: %s, Spout offset is: %s, lag tolerance is: %s" % (spout_json["topology"]["id"],spout_json["topic"],str(partition),offset,spout_json["offset"],lag_tolerance)
          passing.append(msg)
    except:
      exceptions.append("Exception when finding offset for: "+zk_path+" Error: "+traceback.format_exc())
  if exceptions:
    return (('CRITICAL', ['\n'.join(exceptions)]))
  else:
    return (('OK', ['\n'.join(passing)]))

#Utility function for calling commands on the CMD
def run_command(command):
  p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  (output, err) = p.communicate()
  if p.returncode:
    raise Exception('Command: '+command+' returned with non-zero code: '+str(p.returncode)+' stderr: '+err+' stdout: '+output)
  else:
    return output
