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
package kafka.server.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateClusterMirrorOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.StartMirrorTopicsOptions;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.server.config.ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;

/**
 * Integration tests for Cluster Mirroring ACL rules.
 */
public class ClusterMirroringAclsIntegrationTest {
    private static final long METADATA_REFRESH_INTERVAL_MS = 5_000;

    @Test
    void testAclSyncIncludeAndExclude() throws Exception {
        String topic = "acl-topic";
        String mirror = "my-mirror";

        KafkaClusterTestKit aclSrc = buildClusterWithAuthorizer();
        KafkaClusterTestKit aclDst = buildClusterWithAuthorizer();
        try {
            String srcBootstrap = aclSrc.bootstrapServers().split(",")[0];
            try (Admin srcAdmin = Admin.create(Map.of(
                         AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, aclSrc.bootstrapServers()));
                 Admin dstAdmin = Admin.create(Map.of(
                         AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, aclDst.bootstrapServers()))) {

                srcAdmin.createTopics(List.of(
                        new NewTopic(topic, 1, (short) 1)
                )).all().get(10, TimeUnit.SECONDS);

                AclBinding aliceRead = aclBinding(ResourceType.TOPIC, topic, AclOperation.READ, "User:alice");
                AclBinding aliceWrite = aclBinding(ResourceType.TOPIC, topic, AclOperation.WRITE, "User:alice");
                AclBinding bobRead = aclBinding(ResourceType.TOPIC, topic, AclOperation.READ, "User:bob");
                AclBinding groupRead = aclBinding(ResourceType.GROUP, "my-group", AclOperation.READ, "User:alice");
                srcAdmin.createAcls(List.of(aliceRead, aliceWrite, bobRead, groupRead))
                        .all().get(10, TimeUnit.SECONDS);

                dstAdmin.createClusterMirror(mirror, Map.of(
                        ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrap,
                        ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;acl-.*,GROUP;my-group",
                        ClusterMirrorConfig.ACLS_EXCLUDE_CONFIG, "TOPIC;.*;WRITE;ALLOW,ANY;.*;ANY;ANY;User:bob"
                ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
                dstAdmin.startMirrorTopics(mirror, List.of(topic), new StartMirrorTopicsOptions())
                        .all().get(10, TimeUnit.SECONDS);

                waitForCondition(() -> {
                    Collection<AclBinding> dstAcls = dstAdmin.describeAcls(AclBindingFilter.ANY)
                            .values().get(10, TimeUnit.SECONDS);
                    var synced = dstAcls.stream()
                            .filter(a -> a.pattern().resourceType() != ResourceType.CLUSTER_MIRROR)
                            .toList();
                    return synced.contains(aliceRead)
                            && synced.contains(groupRead)
                            && !synced.contains(aliceWrite)
                            && !synced.contains(bobRead);
                }, DEFAULT_MAX_WAIT_MS, "Alice READ+GROUP ACLs should be synced, WRITE and Bob ACLs should be excluded");
            }
        } finally {
            closeQuietly(aclDst);
            closeQuietly(aclSrc);
        }
    }

    @Test
    void testAclExcludeAllOverridesIncludeAll() throws Exception {
        String topic = "exclude-all-topic";
        String mirror = "my-mirror";

        KafkaClusterTestKit aclSrc = buildClusterWithAuthorizer();
        KafkaClusterTestKit aclDst = buildClusterWithAuthorizer();
        try {
            String srcBootstrap = aclSrc.bootstrapServers().split(",")[0];
            try (Admin srcAdmin = Admin.create(Map.of(
                         AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, aclSrc.bootstrapServers()));
                 Admin dstAdmin = Admin.create(Map.of(
                         AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, aclDst.bootstrapServers()))) {

                srcAdmin.createTopics(List.of(
                        new NewTopic(topic, 1, (short) 1)
                )).all().get(10, TimeUnit.SECONDS);

                AclBinding readAcl = aclBinding(ResourceType.TOPIC, topic, AclOperation.READ, "User:alice");
                srcAdmin.createAcls(List.of(readAcl)).all().get(10, TimeUnit.SECONDS);

                dstAdmin.createClusterMirror(mirror, Map.of(
                        ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrap,
                        ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, ".*",
                        ClusterMirrorConfig.ACLS_EXCLUDE_CONFIG, ".*"
                ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
                dstAdmin.startMirrorTopics(mirror, List.of(topic), new StartMirrorTopicsOptions())
                        .all().get(10, TimeUnit.SECONDS);

                // Wait for at least one sync cycle, then verify no ACLs were synced
                Thread.sleep(3 * METADATA_REFRESH_INTERVAL_MS);

                Collection<AclBinding> dstAcls = dstAdmin.describeAcls(AclBindingFilter.ANY)
                        .values().get(10, TimeUnit.SECONDS);
                var synced = dstAcls.stream()
                        .filter(a -> a.pattern().resourceType() != ResourceType.CLUSTER_MIRROR)
                        .toList();
                assert synced.isEmpty() : "No ACLs should be synced when exclude=.*";
            }
        } finally {
            closeQuietly(aclDst);
            closeQuietly(aclSrc);
        }
    }

    private static AclBinding aclBinding(ResourceType resourceType, String resourceName,
                                         AclOperation operation, String principal) {
        return new AclBinding(
                new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
                new AccessControlEntry(principal, "*", operation, AclPermissionType.ALLOW));
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    private KafkaClusterTestKit buildClusterWithAuthorizer() throws Exception {
        var cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp(ClusterMirrorConfig.MIRROR_NUM_REPLICA_FETCHERS_CONFIG, "1")
                .setConfigProp(ClusterMirrorConfig.MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG,
                        String.valueOf(METADATA_REFRESH_INTERVAL_MS))
                .setConfigProp(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false")
                .setConfigProp(ClusterMirrorConfig.MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG, "1")
                .setConfigProp(ClusterMirrorConfig.MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
                .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
                .setConfigProp(DEFAULT_REPLICATION_FACTOR_CONFIG, "1")
                .setConfigProp(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000")
                .setConfigProp(ClusterMirrorConfig.SOCKET_TIMEOUT_MS_CONFIG, "5000")
                .setConfigProp(ClusterMirrorConfig.MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG, 10)
                .setConfigProp(ClusterMirrorConfig.MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG, "100")
                .setConfigProp(ClusterMirrorConfig.MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG, "5000")
                .setConfigProp(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
                .setConfigProp(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")
                .setConfigProp(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG,
                        "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
                .setConfigProp(StandardAuthorizer.SUPER_USERS_CONFIG, "User:ANONYMOUS")
                .build();
        try {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
        } catch (Exception e) {
            closeQuietly(cluster);
            throw e;
        }
        return cluster;
    }
}
