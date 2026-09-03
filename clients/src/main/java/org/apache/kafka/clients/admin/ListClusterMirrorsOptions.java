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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;

/**
 * Options for {@link Admin#listClusterMirrors()}.
 */
@InterfaceStability.Evolving
public class ListClusterMirrorsOptions extends AbstractOptions<ListClusterMirrorsOptions> {
    private List<String> sourceClusterIdFilter;
    private List<String> mirrorNameFilter;
    private List<String> desiredStateFilter;

    /**
     * Filter mirrors by source cluster ID.
     */
    public ListClusterMirrorsOptions sourceClusterIdFilter(List<String> sourceClusterIds) {
        this.sourceClusterIdFilter = sourceClusterIds;
        return this;
    }

    public List<String> sourceClusterIdFilter() {
        return sourceClusterIdFilter;
    }

    /**
     * Filter mirrors by name.
     */
    public ListClusterMirrorsOptions mirrorNameFilter(List<String> mirrorNames) {
        this.mirrorNameFilter = mirrorNames;
        return this;
    }

    public List<String> mirrorNameFilter() {
        return mirrorNameFilter;
    }

    /**
     * Only include topics whose desired state is in this list.
     * Valid values: MIRRORING, PAUSED, STOPPED.
     */
    public ListClusterMirrorsOptions desiredStateFilter(List<String> desiredStates) {
        this.desiredStateFilter = desiredStates;
        return this;
    }

    public List<String> desiredStateFilter() {
        return desiredStateFilter;
    }
}
