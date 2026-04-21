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

import os
import signal

from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka.util import fix_opts_for_new_jvm, get_log4j_config_param, get_log4j_config_for_tools

class TransactionalTestProducer(KafkaPathResolverMixin, BackgroundThreadService):
    """Wraps org.apache.kafka.tools.TransactionalTestProducer for use in
    system testing. Supports three transaction modes:
      commit  - sends records, commits the transaction, then exits.
      abort   - sends records, aborts the transaction, then exits.
      pending - sends records, flushes, then exits without committing
                or aborting, leaving the transaction open on the broker.

    Non-transactional mode is used when transactional_id is None.
    """

    PERSISTENT_ROOT = "/mnt/transactional_test_producer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "transactional_test_producer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "transactional_test_producer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "transactional_test_producer.log")

    logs = {
        "transactional_test_producer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "transactional_test_producer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "transactional_test_producer_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, topic, transactional_id=None,
                 mode="commit", num_records=1, waiting_ms=0):
        super(TransactionalTestProducer, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.topic = topic
        self.transactional_id = transactional_id
        self.mode = mode
        self.num_records = num_records
        self.waiting_ms = waiting_ms
        self.done = False
        self.stop_timeout_sec = 60

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % TransactionalTestProducer.PERSISTENT_ROOT,
                         allow_fail=False)
        log_config = self.render(get_log4j_config_for_tools(node),
                                log_file=TransactionalTestProducer.LOG_FILE)
        node.account.create_file(get_log4j_config_for_tools(node), log_config)

        self.security_config = self.kafka.security_config.client_config(node=node)
        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("TransactionalTestProducer %d command: %s" % (idx, cmd))

        try:
            for line in node.account.ssh_capture(cmd):
                line = line.strip()
                if line == "DONE":
                    with self.lock:
                        self.done = True
                    self.logger.info("TransactionalTestProducer finished (%s)" % self.mode)
        except RemoteCommandError as e:
            self.logger.debug("Got exception reading output, likely SIGKILL'd: %s" % str(e))

    def start_cmd(self, node):
        cmd = "export LOG_DIR=%s;" % TransactionalTestProducer.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"%s%s\"; " % (
            get_log4j_config_param(node), get_log4j_config_for_tools(node))
        cmd += self.path.script("kafka-run-class.sh", node)
        cmd += " org.apache.kafka.tools.TransactionalTestProducer"
        cmd += " --bootstrap-server %s" % self.kafka.bootstrap_servers(
            self.security_config.security_protocol)
        cmd += " --topic %s" % self.topic
        if self.transactional_id is not None:
            cmd += " --transactional-id %s" % self.transactional_id
            cmd += " --mode %s" % self.mode
            if self.waiting_ms > 0:
                cmd += " --waiting-ms %s" % str(self.waiting_ms)
        cmd += " --num-records %s" % str(self.num_records)
        cmd += " 2>> %s | tee -a %s &" % (
            TransactionalTestProducer.STDERR_CAPTURE,
            TransactionalTestProducer.STDOUT_CAPTURE)
        return cmd

    def pids(self, node):
        try:
            cmd = "jps | grep -i TransactionalTestProducer | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(
                cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def kill_node(self, node, clean_shutdown=True):
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL
        for pid in pids:
            node.account.signal(pid, sig)

    def stop_node(self, node):
        self.kill_node(node, clean_shutdown=True)
        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
            (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    @property
    def is_done(self):
        with self.lock:
            return self.done
