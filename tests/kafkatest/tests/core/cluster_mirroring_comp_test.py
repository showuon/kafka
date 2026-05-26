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

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.core.cluster_mirroring_common import ClientService, MirrorConfig, MirrorUtils
from kafkatest.version import (
    CLUSTER_MIRRORING_METADATA_VERSION,
    CLUSTER_MIRRORING_VERSION,
    KafkaVersion,
    LATEST_2_1,
    LATEST_3_9,
    LATEST_4_0,
)


class ClusterMirroringCompPlainTest(MirrorUtils, Test):
    """Tests for KIP-1279 Cluster Mirroring across different Kafka versions."""

    DEST_SERVER_PROPS = [
        ["auto.create.topics.enable", "false"],
        ["default.replication.factor", "2"],
        ["min.insync.replicas", "1"],
        ["offsets.topic.replication.factor", "2"],
        ["transaction.state.log.replication.factor", "2"],
        ["transaction.state.log.min.isr", "1"],
        ["share.coordinator.state.topic.replication.factor", "2"],
        ["share.coordinator.state.topic.min.isr", "1"],
        ["mirror.state.topic.replication.factor", "2"],
    ]

    SOURCE_SERVER_PROPS = [
        ["default.replication.factor", "2"],
        ["min.insync.replicas", "1"],
        ["offsets.topic.replication.factor", "2"],
        ["transaction.state.log.replication.factor", "2"],
        ["transaction.state.log.min.isr", "1"],
    ]

    def __init__(self, test_context):
        super(ClusterMirroringCompPlainTest, self).__init__(test_context)

    def teardown(self):
        if hasattr(self, "_original_metadata_quorum"):
            self.test_context.injected_args["metadata_quorum"] = self._original_metadata_quorum
        for attr in ["dest_kafka", "source_kafka"]:
            kafka = getattr(self, attr, None)
            if kafka is not None:
                try:
                    kafka.stop()
                except Exception:
                    self.logger.warning("Graceful stop failed for %s, forcing SIGKILL" % str(kafka))
                    for node in kafka.nodes:
                        kafka.stop_node(node, clean_shutdown=False)
        for attr in ["dest_client", "source_client"]:
            client = getattr(self, attr, None)
            if client is not None:
                client.stop()
        if hasattr(self, "zk"):
            self.zk.stop()

    def setup_source(self, source_version, metadata_quorum):
        self.source_client = ClientService(self.test_context, version=source_version)
        self.source_client.start()
        self.source_client_node = self.source_client.nodes[0]
        if metadata_quorum == quorum.zk:
            self.zk = ZookeeperService(self.test_context, num_nodes=1)
            self.zk.start()
            self.source_kafka = KafkaService(
                self.test_context, num_nodes=2, zk=self.zk,
                version=source_version,
                server_prop_overrides=self.SOURCE_SERVER_PROPS,
            )
            self._original_metadata_quorum = self.test_context.injected_args.get("metadata_quorum")
            self.test_context.injected_args["metadata_quorum"] = quorum.isolated_kraft
        else:
            self.source_kafka = KafkaService(
                self.test_context, num_nodes=2, zk=None,
                version=source_version,
                controller_num_nodes_override=1,
                server_prop_overrides=self.SOURCE_SERVER_PROPS,
            )
        self.source_kafka.start()

    def setup_dest(self):
        self.dest_client = ClientService(self.test_context)
        self.dest_client.start()
        self.dest_client_node = self.dest_client.nodes[0]
        self.dest_kafka = KafkaService(
            self.test_context, num_nodes=2, zk=None,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=self.DEST_SERVER_PROPS,
        )
        self.dest_kafka.start()
        self.logger.info(
            "Changing metadata.version on %s to %s", self.dest_kafka, CLUSTER_MIRRORING_METADATA_VERSION
        )
        self.dest_kafka.upgrade_metadata_version(CLUSTER_MIRRORING_METADATA_VERSION)
        self.logger.info(
            "Changing mirror.version on %s to %s", self.dest_kafka, CLUSTER_MIRRORING_VERSION
        )
        self.dest_kafka.run_features_command(
            "upgrade", "mirror.version", CLUSTER_MIRRORING_VERSION
        )

#     @cluster(num_nodes=8)
#     @parametrize(source_version=str(LATEST_2_1), metadata_quorum=quorum.zk)
# #         @parametrize(source_version=str(LATEST_3_9), metadata_quorum=quorum.zk)
# #         @parametrize(source_version=str(LATEST_4_0), metadata_quorum=quorum.isolated_kraft)
#     def test_log_convergence_ule(self, metadata_quorum):
#         """Verify log convergence after unclean leader elections and failover/failback."""
#         self.logger.info("Create source topic with ULE support enabled")
#         topic = "my-topic"
#         mirror_name = "new-mirror"
#         self.topics = {topic: {"partitions": 1, "replication-factor": 2}}
#
#         self.source_kafka.create_topic({
#             "topic": topic, "partitions": 1, "replication-factor": 2,
#             "configs": {"mirror.support.unclean.leader.election": "true"},
#         })
#
#         src_broker0 = self.source_kafka.nodes[0]
#         src_broker1 = self.source_kafka.nodes[1]
#
#         def broker_bootstrap(node):
#             """Return bootstrap server address for a single broker node."""
#             return "%s:9092" % node.account.hostname
#
#         def trigger_ule(node):
#             """Trigger unclean leader election on the given node."""
#             cmd = "%s --bootstrap-server %s --topic %s --partition 0 --election-type UNCLEAN" % (
#                 self.source_kafka.path.script("kafka-leader-election.sh", self.client_node),
#                 broker_bootstrap(node), topic)
#             self.client_node.account.ssh(cmd, allow_fail=False)
#
#         def log_hashes(label):
#             """Log MD5 hashes of partition log segments for all brokers across both clusters."""
#             self.logger.info("#### %s-0 %s ####", topic, label)
#             for name, kafka in [("source", self.source_kafka), ("dest", self.dest_kafka)]:
#                 for node in kafka.nodes:
#                     cmd = "md5sum %s*/%s-0/*.log 2>/dev/null" % (
#                         KafkaService.DATA_LOG_DIR_PREFIX, topic)
#                     lines = list(node.account.ssh_capture(cmd, allow_fail=True))
#                     if lines:
#                         for line in lines:
#                             self.logger.info("%s %s: %s", name, node.name, line.strip())
#                     else:
#                         self.logger.info("%s %s: n/a", name, node.name)
#
#         self.logger.info("Bounce source brokers to trigger leader elections")
#         self.source_kafka.restart_cluster(clean_shutdown=True)
#
#         self.logger.info("Send 1 message via source broker 0")
#         self.produce_records(self.source_kafka, topic, 1, self.client_node,
#                              bootstrap_servers=broker_bootstrap(src_broker0))
#
#         self.logger.info("Start cluster mirror on destination")
#         mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
#
#         wait_until(
#             lambda: self.dest_kafka.create_cluster_mirror(
#                 self.client_node, mirror_name, mirror_cfg),
#             timeout_sec=300, backoff_sec=2,
#             err_msg="Failed to create cluster mirror",
#         )
#         wait_until(
#             lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
#                 self.client_node, mirror_name, topic),
#             timeout_sec=300, backoff_sec=2,
#             err_msg="Failed to start mirror topics",
#         )
#         self.wait_mirror_state(
#             self.dest_kafka, mirror_name, "MIRRORING", [topic],
#             err_msg="Mirror did not reach MIRRORING state",
#         )
#
#         self.logger.info("Stop source broker 0 (broker 0 becomes stale)")
#         self.source_kafka.stop_node(src_broker0)
#
#         self.logger.info("Send 1 message via source broker 1")
#         self.produce_records(self.source_kafka, topic, 1, self.client_node,
#                              bootstrap_servers=broker_bootstrap(src_broker1))
#         self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic],
#                                   err_msg="Mirror did not catch up after broker 0 stopped")
#         log_hashes("after source broker 0 stopped (broker 0 should be out of sync)")
#
#         self.logger.info("ULE 1: stop broker 1, start broker 0 (stale), elect it as leader")
#         self.source_kafka.stop_node(src_broker1)
#         self.source_kafka.start_node(src_broker0)
#         time.sleep(5)
#         trigger_ule(src_broker0)
#
#         self.logger.info("Send 2 messages via source broker 0")
#         self.produce_records(self.source_kafka, topic, 2, self.client_node,
#                              bootstrap_servers=broker_bootstrap(src_broker0))
#         self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic],
#                                   err_msg="Mirror did not catch up after ULE 1")
#         log_hashes("after ULE 1 (broker 1 should be out of sync)")
#
#         self.logger.info("Failover: stop mirror so destination topic becomes writable")
#         self.dest_kafka.stop_cluster_mirror_topics(self.client_node, mirror_name, topic)
#         self.wait_mirror_state(self.dest_kafka, mirror_name, "STOPPED", [topic])
#         self.logger.info("describe_cluster_mirror: %s",
#                          self.dest_kafka.describe_cluster_mirror(self.client_node))



    @cluster(num_nodes=8)
    @parametrize(source_version=str(LATEST_2_1), metadata_quorum=quorum.zk)
#     @parametrize(source_version=str(LATEST_3_9), metadata_quorum=quorum.zk)
#     @parametrize(source_version=str(LATEST_4_0), metadata_quorum=quorum.isolated_kraft)
    def test_mirroring(self, source_version, metadata_quorum):
        """Verify log convergence after unclean leader elections and failover/failback."""
        self.logger.info("Create source topic with ULE support enabled")
        topic = "my-topic"
        mirror_name = "new-mirror"
        self.topics = {topic: {"partitions": 1, "replication-factor": 2}}

        self.setup_source(KafkaVersion(source_version), metadata_quorum)
        self.setup_dest()

        self.source_kafka.create_topic({
            "topic": topic, "partitions": 1, "replication-factor": 2
        })

        src_broker0 = self.source_kafka.nodes[0]
        src_broker1 = self.source_kafka.nodes[1]

        def broker_bootstrap(node):
            """Return bootstrap server address for a single broker node."""
            return "%s:9092" % node.account.hostname

        def trigger_ule(node):
            """Trigger unclean leader election on the given node."""
            cmd = "%s --bootstrap-server %s --topic %s --partition 0 --election-type UNCLEAN" % (
                self.source_kafka.path.script("kafka-leader-election.sh", self.client_node),
                broker_bootstrap(node), topic)
            self.client_node.account.ssh(cmd, allow_fail=False)

        def log_hashes(label):
            """Log MD5 hashes of partition log segments for all brokers across both clusters."""
            self.logger.info("#### %s-0 %s ####", topic, label)
            for name, kafka in [("source", self.source_kafka), ("dest", self.dest_kafka)]:
                for node in kafka.nodes:
                    cmd = "md5sum %s*/%s-0/*.log 2>/dev/null" % (
                        KafkaService.DATA_LOG_DIR_PREFIX, topic)
                    lines = list(node.account.ssh_capture(cmd, allow_fail=True))
                    if lines:
                        for line in lines:
                            self.logger.info("%s %s: %s", name, node.name, line.strip())
                    else:
                        self.logger.info("%s %s: n/a", name, node.name)

        self.logger.info("Bounce source brokers to trigger leader elections")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        self.logger.info("Send 1 message via source broker 0")
        self.produce_records(self.source_kafka, topic, 1, self.source_client_node,
                             bootstrap_servers=broker_bootstrap(src_broker0))

        self.logger.info("Start cluster mirror on destination")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.dest_client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.dest_client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_state(
            self.dest_kafka, mirror_name, "MIRRORING", [topic],
            err_msg="Mirror did not reach MIRRORING state",
        )

        self.logger.info("Stop source broker 0 (broker 0 becomes stale)")
        self.source_kafka.stop_node(src_broker0)

        self.logger.info("Send 1 message via source broker 1")
        self.produce_records(self.source_kafka, topic, 1, self.source_client_node,
                             bootstrap_servers=broker_bootstrap(src_broker1))
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic],
                                  err_msg="Mirror did not catch up after broker 0 stopped")
        log_hashes("after source broker 0 stopped (broker 0 should be out of sync)")

        self.logger.info("ULE 1: stop broker 1, start broker 0 (stale), elect it as leader")
        self.source_kafka.stop_node(src_broker1)
        self.source_kafka.start_node(src_broker0)
        time.sleep(5)
        trigger_ule(src_broker0)

        self.logger.info("Send 2 messages via source broker 0")
        self.produce_records(self.source_kafka, topic, 2, self.source_client_node,
                             bootstrap_servers=broker_bootstrap(src_broker0))
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic],
                                  err_msg="Mirror did not catch up after ULE 1")
        log_hashes("after ULE 1 (broker 1 should be out of sync)")

        self.logger.info("Failover: stop mirror so destination topic becomes writable")
        self.dest_kafka.stop_cluster_mirror_topics(self.dest_client_node, mirror_name, topic)
        self.wait_mirror_state(self.dest_kafka, mirror_name, "STOPPED", [topic])
        self.logger.info("describe_cluster_mirror: %s",
                         self.dest_kafka.describe_cluster_mirror(self.dest_client_node))

