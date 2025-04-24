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

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster

from kafkatest.tests.verifiable_consumer_test import VerifiableConsumerTest
from kafkatest.services.kafka import TopicPartition, quorum, consumer_group

import signal


class OffsetValidationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 1

    def __init__(self, test_context):
        super(OffsetValidationTest, self).__init__(test_context, num_consumers=3, num_producers=1,
                                                     num_zk=1, num_brokers=2, topics={
            self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 2 }
        })

    def rolling_bounce_consumers(self, consumer, keep_alive=0, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes[keep_alive:]:
                consumer.stop_node(node, clean_shutdown)

                wait_until(lambda: len(consumer.dead_nodes()) == 1,
                           timeout_sec=self.session_timeout_sec+5,
                           err_msg="Timed out waiting for the consumer to shutdown")

                consumer.start_node(node)

                self.await_all_members(consumer)
                self.await_consumed_messages(consumer)

    def bounce_all_consumers(self, consumer, keep_alive=0, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes[keep_alive:]:
                consumer.stop_node(node, clean_shutdown)

            wait_until(lambda: len(consumer.dead_nodes()) == self.num_consumers - keep_alive, timeout_sec=10,
                       err_msg="Timed out waiting for the consumers to shutdown")

            for node in consumer.nodes[keep_alive:]:
                consumer.start_node(node)

            self.await_all_members(consumer)
            self.await_consumed_messages(consumer)

    def rolling_bounce_brokers(self, consumer, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in self.kafka.nodes:
                self.kafka.restart_node(node, clean_shutdown=True)
                self.await_all_members(consumer)
                self.await_consumed_messages(consumer)

    def setup_consumer(self, topic, **kwargs):
        # collect verifiable consumer events since this makes debugging much easier
        consumer = super(OffsetValidationTest, self).setup_consumer(topic, **kwargs)
        self.mark_for_collect(consumer, 'verifiable_consumer_stdout')
        return consumer

    @cluster(num_nodes=7)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_broker_rolling_bounce(self, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        """
        Verify correct consumer behavior when the brokers are consecutively restarted.

        Setup: single Kafka cluster with one producer writing messages to a single topic with one
        partition, an a set of consumers in the same group reading from the same topic.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers and wait until they've joined the group.
        - In a loop, restart each broker consecutively, waiting for the group to stabilize between
          each broker restart.
        - Verify delivery semantics according to the failure type and that the broker bounces
          did not cause unexpected group rebalances.
        """
        partition = TopicPartition(self.TOPIC, 0)

        producer = self.setup_producer(self.TOPIC)
        # The consumers' session timeouts must exceed the time it takes for a broker to roll.  Consumers are likely
        # to see cluster metadata consisting of just a single alive broker in the case where the cluster has just 2
        # brokers and the cluster is rolling (which is what is happening here).  When the consumer sees a single alive
        # broker, and then that broker rolls, the consumer will be unable to connect to the cluster until that broker
        # completes its roll.  In the meantime, the consumer group will move to the group coordinator on the other
        # broker, and that coordinator will fail the consumer and trigger a group rebalance if its session times out.
        # This test is asserting that no rebalances occur, so we increase the session timeout for this to be the case.
        self.session_timeout_sec = 30
        consumer = self.setup_consumer(self.TOPIC, group_protocol=group_protocol)

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)

        num_rebalances = consumer.num_rebalances()
        # TODO: make this test work with hard shutdowns, which probably requires
        #       pausing before the node is restarted to ensure that any ephemeral
        #       nodes have time to expire
        self.rolling_bounce_brokers(consumer, clean_shutdown=True)

        unexpected_rebalances = consumer.num_rebalances() - num_rebalances
        assert unexpected_rebalances == 0, \
            "Broker rolling bounce caused %d unexpected group rebalances" % unexpected_rebalances

        consumer.stop_all()

        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records %d did not match consumed position %d" % \
            (consumer.total_consumed(), consumer.current_position(partition))




    @cluster(num_nodes=10)
    @matrix(
        num_conflict_consumers=[1, 2],
        fencing_stage=["stable"],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_fencing_static_consumer(self, num_conflict_consumers, fencing_stage, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        """
        Verify correct static consumer behavior when there are conflicting consumers with same group.instance.id.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers as static members and wait until they've joined the group. Some conflict consumers will be configured with
        - the same group.instance.id.
        - Let normal consumers and fencing consumers start at the same time, and expect only unique consumers left.
        """
        partition = TopicPartition(self.TOPIC, 0)

        producer = self.setup_producer(self.TOPIC)

        producer.start()
        self.await_produced_messages(producer)

        self.session_timeout_sec = 60
        consumer = self.setup_consumer(self.TOPIC, static_membership=True, group_protocol=group_protocol)

        self.num_consumers = num_conflict_consumers
        conflict_consumer = self.setup_consumer(self.TOPIC, static_membership=True, group_protocol=group_protocol)

        # wait original set of consumer to stable stage before starting conflict members.
        if fencing_stage == "stable":
            consumer.start()
            self.await_members(consumer, len(consumer.nodes))

            num_rebalances = consumer.num_rebalances()
            conflict_consumer.start()
            if group_protocol == consumer_group.classic_group_protocol:
                # Classic protocol: conflicting members should join, and the intial ones with conflicting instance id should fail.
                self.await_members(conflict_consumer, num_conflict_consumers)
                self.await_members(consumer, len(consumer.nodes) - num_conflict_consumers)

                wait_until(lambda: len(consumer.dead_nodes()) == num_conflict_consumers,
                       timeout_sec=10,
                       err_msg="Timed out waiting for the fenced consumers to stop")
            else:
                # Consumer protocol: Existing members should remain active and new conflicting ones should not be able to join.
                self.await_consumed_messages(consumer)
                assert num_rebalances == consumer.num_rebalances(), "Static consumers attempt to join with instance id in use should not cause a rebalance"
                assert len(consumer.joined_nodes()) == len(consumer.nodes)
                assert len(conflict_consumer.joined_nodes()) == 0
                
                # Stop existing nodes, so conflicting ones should be able to join.
                consumer.stop_all()
                wait_until(lambda: len(consumer.dead_nodes()) == len(consumer.nodes),
                           timeout_sec=self.session_timeout_sec+5,
                           err_msg="Timed out waiting for the consumer to shutdown")
                conflict_consumer.start()
                self.await_members(conflict_consumer, num_conflict_consumers)

            
        else:
            consumer.start()
            conflict_consumer.start()

            wait_until(lambda: len(consumer.joined_nodes()) + len(conflict_consumer.joined_nodes()) == len(consumer.nodes),
                       timeout_sec=self.session_timeout_sec*2,
                       err_msg="Timed out waiting for consumers to join, expected total %d joined, but only see %d joined from "
                               "normal consumer group and %d from conflict consumer group" % \
                               (len(consumer.nodes), len(consumer.joined_nodes()), len(conflict_consumer.joined_nodes()))
                       )
            wait_until(lambda: len(consumer.dead_nodes()) + len(conflict_consumer.dead_nodes()) == len(conflict_consumer.nodes),
                       timeout_sec=self.session_timeout_sec*2,
                       err_msg="Timed out waiting for fenced consumers to die, expected total %d dead, but only see %d dead in "
                               "normal consumer group and %d dead in conflict consumer group" % \
                               (len(conflict_consumer.nodes), len(consumer.dead_nodes()), len(conflict_consumer.dead_nodes()))
                       )

    @cluster(num_nodes=7)
    @matrix(
        clean_shutdown=[True],
        enable_autocommit=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_consumer_failure(self, clean_shutdown, enable_autocommit, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        partition = TopicPartition(self.TOPIC, 0)

        consumer = self.setup_consumer(self.TOPIC, enable_autocommit=enable_autocommit, group_protocol=group_protocol)
        producer = self.setup_producer(self.TOPIC)

        consumer.start()
        self.await_all_members(consumer)

        partition_owner = consumer.owner(partition)
        assert partition_owner is not None

        # startup the producer and ensure that some records have been written
        producer.start()
        self.await_produced_messages(producer)

        # stop the partition owner and await its shutdown
        consumer.kill_node(partition_owner, clean_shutdown=clean_shutdown)
        wait_until(lambda: len(consumer.joined_nodes()) == (self.num_consumers - 1) and consumer.owner(partition) is not None,
                   timeout_sec=self.session_timeout_sec*2+5,
                   err_msg="Timed out waiting for consumer to close")

        # ensure that the remaining consumer does some work after rebalancing
        self.await_consumed_messages(consumer, min_messages=1000)

        consumer.stop_all()

        if clean_shutdown:
            # if the total records consumed matches the current position, we haven't seen any duplicates
            # this can only be guaranteed with a clean shutdown
            assert consumer.current_position(partition) == consumer.total_consumed(), \
                "Total consumed records %d did not match consumed position %d" % \
                (consumer.total_consumed(), consumer.current_position(partition))
        else:
            # we may have duplicates in a hard failure
            assert consumer.current_position(partition) <= consumer.total_consumed(), \
                "Current position %d greater than the total number of consumed records %d" % \
                (consumer.current_position(partition), consumer.total_consumed())

        # if autocommit is not turned on, we can also verify the last committed offset
        if not enable_autocommit:
            assert consumer.last_commit(partition) == consumer.current_position(partition), \
                "Last committed offset %d did not match last consumed position %d" % \
                (consumer.last_commit(partition), consumer.current_position(partition))

    @cluster(num_nodes=7)
    @matrix(
        clean_shutdown=[True],
        enable_autocommit=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_broker_failure(self, clean_shutdown, enable_autocommit, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        partition = TopicPartition(self.TOPIC, 0)

        consumer = self.setup_consumer(self.TOPIC, enable_autocommit=enable_autocommit, group_protocol=group_protocol)
        producer = self.setup_producer(self.TOPIC)

        producer.start()
        consumer.start()
        self.await_all_members(consumer)

        num_rebalances = consumer.num_rebalances()

        # shutdown one of the brokers
        # TODO: we need a way to target the coordinator instead of picking arbitrarily
        self.kafka.signal_node(self.kafka.nodes[0], signal.SIGTERM if clean_shutdown else signal.SIGKILL)

        # ensure that the consumers do some work after the broker failure
        self.await_consumed_messages(consumer, min_messages=1000)

        # verify that there were no rebalances on failover
        assert num_rebalances == consumer.num_rebalances(), "Broker failure should not cause a rebalance"

        consumer.stop_all()

        # if the total records consumed matches the current position, we haven't seen any duplicates
        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records %d did not match consumed position %d" % \
            (consumer.total_consumed(), consumer.current_position(partition))

        # if autocommit is not turned on, we can also verify the last committed offset
        if not enable_autocommit:
            assert consumer.last_commit(partition) == consumer.current_position(partition), \
                "Last committed offset %d did not match last consumed position %d" % \
                (consumer.last_commit(partition), consumer.current_position(partition))


class AssignmentValidationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 6

    def __init__(self, test_context):
        super(AssignmentValidationTest, self).__init__(test_context, num_consumers=3, num_producers=0,
                                                num_zk=1, num_brokers=2, topics={
            self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 1 },
        })

