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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A listing of a cluster mirror.
 */
public class ClusterMirrorListing {
    private final String mirrorName;
    private final String sourceBootstrap;
    private final String sourceClusterId;
    private final List<String> topicNames;

    public ClusterMirrorListing(String mirrorName, String sourceBootstrap, String sourceClusterId, List<String> topicNames) {
        this.mirrorName = mirrorName;
        this.sourceBootstrap = sourceBootstrap;
        this.sourceClusterId = sourceClusterId;
        this.topicNames = topicNames;
    }

    public ClusterMirrorListing(String mirrorName, String sourceBootstrap, String sourceClusterId) {
        this(mirrorName, sourceBootstrap, sourceClusterId, Collections.emptyList());
    }

    public ClusterMirrorListing(String mirrorName, String sourceBootstrap) {
        this(mirrorName, sourceBootstrap, "", Collections.emptyList());
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

    /**
     * The topic names configured for this mirror,
     * filtered by desired state if a filter was set in the request.
     */
    public List<String> topicNames() {
        return topicNames;
    }

    @Override
    public String toString() {
        return "ClusterMirrorListing(mirrorName='" + mirrorName + "', sourceBootstrap='" + sourceBootstrap
                + "', sourceClusterId='" + sourceClusterId + "', topicNames=" + topicNames + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(mirrorName, sourceBootstrap, sourceClusterId, topicNames);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterMirrorListing)) return false;
        ClusterMirrorListing that = (ClusterMirrorListing) o;
        return Objects.equals(mirrorName, that.mirrorName) &&
               Objects.equals(sourceBootstrap, that.sourceBootstrap) &&
               Objects.equals(sourceClusterId, that.sourceClusterId) &&
               Objects.equals(topicNames, that.topicNames);
    }
}
