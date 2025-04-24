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

from ducktape.tests.test import Test
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.mark import matrix, parametrize
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, config_property, quorum, consumer_group
from kafkatest.services.connect import ConnectDistributedService, ConnectServiceBase, VerifiableSource, VerifiableSink, ConnectRestError, MockSink, MockSource
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH, LATEST_2_3, LATEST_2_2, LATEST_2_1, LATEST_2_0, LATEST_1_1, LATEST_1_0, LATEST_0_11_0, LATEST_0_10_2, LATEST_0_10_1, LATEST_0_10_0, LATEST_0_9, LATEST_0_8_2, KafkaVersion

from functools import reduce
from collections import Counter, namedtuple
import itertools
import json
import operator
import time

class ConnectDistributedTest(Test):
    """
    Simple test of Kafka Connect in distributed mode, producing data from files on one cluster and consuming it on
    another, validating the total output is identical to the input.
    """

    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    INPUT_FILE = "/mnt/connect.input"
    OUTPUT_FILE = "/mnt/connect.output"

    TOPIC = "test"
    OFFSETS_TOPIC = "connect-offsets"
    OFFSETS_REPLICATION_FACTOR = "1"
    OFFSETS_PARTITIONS = "1"
    CONFIG_TOPIC = "connect-configs"
    CONFIG_REPLICATION_FACTOR = "1"
    STATUS_TOPIC = "connect-status"
    STATUS_REPLICATION_FACTOR = "1"
    STATUS_PARTITIONS = "1"
    EXACTLY_ONCE_SOURCE_SUPPORT = "disabled"
    SCHEDULED_REBALANCE_MAX_DELAY_MS = "60000"
    CONNECT_PROTOCOL="sessioned"

    # Since tasks can be assigned to any node and we're testing with files, we need to make sure the content is the same
    # across all nodes.
    FIRST_INPUT_LIST = ["foo", "bar", "baz"]
    FIRST_INPUTS = "\n".join(FIRST_INPUT_LIST) + "\n"
    SECOND_INPUT_LIST = ["razz", "ma", "tazz"]
    SECOND_INPUTS = "\n".join(SECOND_INPUT_LIST) + "\n"

    SCHEMA = { "type": "string", "optional": False }

    def __init__(self, test_context):
        super(ConnectDistributedTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.topics = {
            self.TOPIC: {'partitions': 1, 'replication-factor': 1}
        }

        self.zk = ZookeeperService(test_context, self.num_zk) if quorum.for_test(test_context) == quorum.zk else None

        self.key_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.value_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.schemas = True

    def setup_services(self,
                       security_protocol=SecurityConfig.PLAINTEXT,
                       timestamp_type=None,
                       broker_version=DEV_BRANCH,
                       auto_create_topics=False,
                       include_filestream_connectors=False,
                       num_workers=3):
        self.kafka = KafkaService(self.test_context, self.num_brokers, self.zk,
                                  security_protocol=security_protocol, interbroker_security_protocol=security_protocol,
                                  topics=self.topics, version=broker_version,
                                  server_prop_overrides=[
                                      ["auto.create.topics.enable", str(auto_create_topics)],
                                      ["transaction.state.log.replication.factor", str(self.num_brokers)],
                                      ["transaction.state.log.min.isr", str(self.num_brokers)]
                                  ])
        if timestamp_type is not None:
            for node in self.kafka.nodes:
                node.config[config_property.MESSAGE_TIMESTAMP_TYPE] = timestamp_type

        self.cc = ConnectDistributedService(self.test_context, num_workers, self.kafka, [self.INPUT_FILE, self.OUTPUT_FILE],
                                            include_filestream_connectors=include_filestream_connectors)
        self.cc.log_level = "DEBUG"

        if self.zk:
            self.zk.start()
        self.kafka.start()

    def _start_connector(self, config_file, extra_config={}):
        connector_props = self.render(config_file)
        connector_config = dict([line.strip().split('=', 1) for line in connector_props.split('\n') if line.strip() and not line.strip().startswith('#')])
        connector_config.update(extra_config)
        self.cc.create_connector(connector_config)
            
    def _connector_status(self, connector, node=None):
        try:
            return self.cc.get_connector_status(connector, node)
        except ConnectRestError:
            return None

    def _connector_has_state(self, status, state):
        return status is not None and status['connector']['state'] == state

    def _task_has_state(self, task_id, status, state):
        if not status:
            return False

        tasks = status['tasks']
        if not tasks:
            return False

        for task in tasks:
            if task['id'] == task_id:
                return task['state'] == state

        return False

    def _all_tasks_have_state(self, status, task_count, state):
        if status is None:
            return False

        tasks = status['tasks']
        if len(tasks) != task_count:
            return False

        return reduce(operator.and_, [task['state'] == state for task in tasks], True)

    def is_running(self, connector, node=None):
        status = self._connector_status(connector.name, node)
        return self._connector_has_state(status, 'RUNNING') and self._all_tasks_have_state(status, connector.tasks, 'RUNNING')

    def is_paused(self, connector, node=None):
        status = self._connector_status(connector.name, node)
        return self._connector_has_state(status, 'PAUSED') and self._all_tasks_have_state(status, connector.tasks, 'PAUSED')

    def connector_is_running(self, connector, node=None):
        status = self._connector_status(connector.name, node)
        return self._connector_has_state(status, 'RUNNING')

    def connector_is_failed(self, connector, node=None):
        status = self._connector_status(connector.name, node)
        return self._connector_has_state(status, 'FAILED')

    def task_is_failed(self, connector, task_id, node=None):
        status = self._connector_status(connector.name, node)
        return self._task_has_state(task_id, status, 'FAILED')

    def task_is_running(self, connector, task_id, node=None):
        status = self._connector_status(connector.name, node)
        return self._task_has_state(task_id, status, 'RUNNING')


    def _different_level(self, current_level):
        return 'INFO' if current_level is None or current_level.upper() != 'INFO' else 'WARN'

    def _set_logger(self, worker, namespace, new_level, scope=None):
        """
        Set a log level via the PUT /admin/loggers/{logger} endpoint, verify that the response
        has the expected format, and then return the time at which the request was issued.
        :param worker: the worker to issue the REST request to
        :param namespace: the logging namespace to adjust
        :param new_level: the new level for the namespace
        :param scope: the scope of the logging adjustment; if None, then no scope will be specified
        in the REST request
        :return: the time at or directly before which the REST request was made
        """
        request_time = int(time.time() * 1000)
        affected_loggers = self.cc.set_logger(worker, namespace, new_level, scope)
        if scope is not None and scope.lower() == 'cluster':
            assert affected_loggers is None
        else:
            assert len(affected_loggers) >= 1
            for logger in affected_loggers:
                assert logger.startswith(namespace)
        return request_time

    def _loggers_are_set(self, expected_level, last_modified, namespace, workers=None):
        """
        Verify that all loggers for a namespace (as returned from the GET /admin/loggers endpoint) have
        an expected level and last-modified timestamp.
        :param expected_level: the expected level for all loggers in the namespace
        :param last_modified: the expected last modified timestamp; if None, then all loggers
        are expected to have null timestamps; otherwise, all loggers are expected to have timestamps
        greater than or equal to this value
        :param namespace: the logging namespace to examine
        :param workers: the workers to query
        :return: whether the expected logging levels and last-modified timestamps are set
        """
        if workers is None:
            workers = self.cc.nodes
        for worker in workers:
            all_loggers = self.cc.get_all_loggers(worker)
            self.logger.debug("Read loggers on %s from Connect REST API: %s", str(worker), str(all_loggers))
            namespaced_loggers = {k: v for k, v in all_loggers.items() if k.startswith(namespace)}
            if len(namespaced_loggers) < 1:
                return False
            for logger in namespaced_loggers.values():
                if logger['level'] != expected_level:
                    return False
                if last_modified is None:
                    # Fail fast if there's a non-null timestamp; it'll never be reset to null
                    assert logger['last_modified'] is None
                elif logger['last_modified'] is None or logger['last_modified'] < last_modified:
                    return False
        return True

    def _wait_for_loggers(self, level, request_time, namespace, workers=None):
        wait_until(
            lambda: self._loggers_are_set(level, request_time, namespace, workers),
            # This should be super quick--just a write+read of the config topic, which workers are constantly polling
            timeout_sec=10,
            err_msg="Log level for namespace '" + namespace + "'  was not adjusted in a reasonable amount of time."
        )

    @cluster(num_nodes=6)
    @matrix(
        security_protocol=[SecurityConfig.SASL_SSL],
        exactly_once_source=[True],
        connect_protocol=['eager'],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_file_source_and_sink(self, security_protocol, exactly_once_source, connect_protocol, metadata_quorum, use_new_coordinator=False, group_protocol=None):
        """
        Tests that a basic file connector works across clean rolling bounces. This validates that the connector is
        correctly created, tasks instantiated, and as nodes restart the work is rebalanced across nodes.
        """

        self.EXACTLY_ONCE_SOURCE_SUPPORT = 'enabled' if exactly_once_source else 'disabled'
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services(security_protocol=security_protocol, include_filestream_connectors=True)
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))

        self.cc.start()

        self.logger.info("Creating connectors")
        self._start_connector("connect-file-source.properties")
        if group_protocol is not None:
            self._start_connector("connect-file-sink.properties", {"consumer.override.group.protocol" : group_protocol})
        else:
            self._start_connector("connect-file-sink.properties")
        
        # Generating data on the source node should generate new records and create new output on the sink node. Timeouts
        # here need to be more generous than they are for standalone mode because a) it takes longer to write configs,
        # do rebalancing of the group, etc, and b) without explicit leave group support, rebalancing takes awhile
        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.FIRST_INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self._validate_file_output(self.FIRST_INPUT_LIST), timeout_sec=70, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        # Restarting both should result in them picking up where they left off,
        # only processing new data.
        self.cc.restart()

        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.SECOND_INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self._validate_file_output(self.FIRST_INPUT_LIST + self.SECOND_INPUT_LIST), timeout_sec=150, err_msg="Sink output file never converged to the same state as the input file")


    def _validate_file_output(self, input):
        input_set = set(input)
        # Output needs to be collected from all nodes because we can't be sure where the tasks will be scheduled.
        # Between the first and second rounds, we might even end up with half the data on each node.
        output_set = set(itertools.chain(*[
            [line.strip() for line in self._file_contents(node, self.OUTPUT_FILE)] for node in self.cc.nodes
        ]))
        return input_set == output_set

    def _file_contents(self, node, file):
        try:
            # Convert to a list here or the RemoteCommandError may be returned during a call to the generator instead of
            # immediately
            return list(node.account.ssh_capture("cat " + file))
        except RemoteCommandError:
            return []

    def _restart_worker(self, node, clean=True):
        started = time.time()
        self.logger.info("%s bouncing Kafka Connect on %s", clean and "Clean" or "Hard", str(node.account))
        self.cc.stop_node(node, clean_shutdown=clean, await_shutdown=True)
        with node.account.monitor_log(self.cc.LOG_FILE) as monitor:
            self.cc.start_node(node)
            monitor.wait_until("Starting connectors and tasks using config offset", timeout_sec=90,
                               err_msg="Kafka Connect worker didn't successfully join group and start work")
        self.logger.info("Bounced Kafka Connect on %s and rejoined in %f seconds", node.account, time.time() - started)
