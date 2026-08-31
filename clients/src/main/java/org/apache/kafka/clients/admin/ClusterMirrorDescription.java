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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A detailed description of a cluster mirror.
 */
public class ClusterMirrorDescription {
    private final String mirrorName;
    private final String sourceBootstrap;
    private final String sourceClusterId;
    private final Map<String, Set<LeaderStateDescription>> leaderStates;
    private final Set<AclOperation> authorizedOperations;

    public ClusterMirrorDescription(String mirrorName,
                                    String sourceBootstrap,
                                    String sourceClusterId,
                                    Map<String, Set<LeaderStateDescription>> leaderStates,
                                    Set<AclOperation> authorizedOperations) {
        this.mirrorName = mirrorName;
        this.sourceBootstrap = sourceBootstrap;
        this.sourceClusterId = sourceClusterId;
        this.leaderStates = Collections.unmodifiableMap(leaderStates);
        this.authorizedOperations = authorizedOperations;
    }

    public String mirrorName() {
        return mirrorName;
    }

    public String sourceBootstrap() {
        return sourceBootstrap;
    }

    public String sourceClusterId() {
        return sourceClusterId;
    }

    public Map<String, Set<LeaderStateDescription>> leaderStates() {
        return leaderStates;
    }

    public Set<AclOperation> authorizedOperations() {
        return authorizedOperations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterMirrorDescription that = (ClusterMirrorDescription) o;
        return Objects.equals(mirrorName, that.mirrorName) &&
               Objects.equals(sourceBootstrap, that.sourceBootstrap) &&
               Objects.equals(sourceClusterId, that.sourceClusterId) &&
               Objects.equals(leaderStates, that.leaderStates) &&
               Objects.equals(authorizedOperations, that.authorizedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mirrorName, sourceBootstrap, sourceClusterId, leaderStates, authorizedOperations);
    }

    @Override
    public String toString() {
        return "ClusterMirrorDescription{" +
               "mirrorName='" + mirrorName + '\'' +
               ", sourceBootstrap='" + sourceBootstrap + '\'' +
               ", sourceClusterId='" + sourceClusterId + '\'' +
               ", leaderStates=" + leaderStates +
               ", authorizedOperations=" + authorizedOperations +
               '}';
    }

    /** Represents the mirroring state of a leader partition. */
    public static class LeaderStateDescription {
        private final TopicPartition topicPartition;
        private final long sourceOffset;
        private final long destinationOffset;
        private final String state;
        private final int retryAttempt;
        private final String errorMessage;

        public LeaderStateDescription(TopicPartition topicPartition, long sourceOffset, long destinationOffset,
                                      String state, int retryAttempt, String errorMessage) {
            this.topicPartition = topicPartition;
            this.sourceOffset = sourceOffset;
            this.destinationOffset = destinationOffset;
            this.state = state;
            this.retryAttempt = retryAttempt;
            this.errorMessage = errorMessage;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        public long sourceOffset() {
            return sourceOffset;
        }

        public long destinationOffset() {
            return destinationOffset;
        }

        /** Replication lag: max(0, sourceOffset - destinationOffset), or -1 if offsets are unavailable. */
        public long lag() {
            if (sourceOffset < 0 || destinationOffset < 0) {
                return -1;
            }
            return Math.max(0, sourceOffset - destinationOffset);
        }

        /**
         * The mirror partition state: LOG_ALIGNMENT, EPOCH_FENCING, ULE_RECOVERY,
         * MIRRORING, PAUSING, PAUSED, STOPPING, STOPPED, FAILED, or UNKNOWN.
         */
        public String state() {
            return state;
        }

        public int retryAttempt() {
            return retryAttempt;
        }

        public String errorMessage() {
            return errorMessage;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LeaderStateDescription that = (LeaderStateDescription) o;
            return sourceOffset == that.sourceOffset &&
                   destinationOffset == that.destinationOffset &&
                   retryAttempt == that.retryAttempt &&
                   Objects.equals(topicPartition, that.topicPartition) &&
                   Objects.equals(state, that.state) &&
                   Objects.equals(errorMessage, that.errorMessage);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicPartition, sourceOffset, destinationOffset, state, retryAttempt, errorMessage);
        }

        @Override
        public String toString() {
            return "LeaderStateDescription{" +
                   "topicPartition=" + topicPartition +
                   ", sourceOffset=" + sourceOffset +
                   ", destinationOffset=" + destinationOffset +
                   ", state='" + state + '\'' +
                   ", retryAttempt=" + retryAttempt +
                   ", errorMessage='" + errorMessage + '\'' +
                   '}';
        }
    }
}
