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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * This key is used to uniquely identify a cluster mirror by its system-generated ID.
 * The key format is {@code mirrorId:topicId:partition} where all segments are UUID/int,
 * eliminating the colon-ambiguity problem that existed with user-provided mirror names.
 */
public record MirrorRecordKey(Uuid mirrorId, Uuid topicId, int partition) {
    public MirrorRecordKey(Uuid mirrorId, Uuid topicId, int partition) {
        this.mirrorId = Objects.requireNonNull(mirrorId, "Mirror ID cannot be null");
        this.topicId = Objects.requireNonNull(topicId, "topicId cannot be null");
        this.partition = partition;
    }

    public static MirrorRecordKey getInstance(String key) {
        validate(key);
        String[] tokens = key.split(":");
        return new MirrorRecordKey(
                Uuid.fromString(tokens[0].trim()),
                Uuid.fromString(tokens[1]),
                Integer.parseInt(tokens[2])
        );
    }

    public String asCoordinatorKey() {
        return asCoordinatorKey(mirrorId, topicId, partition);
    }

    public static String asCoordinatorKey(Uuid mirrorId, Uuid topicId, int partition) {
        return String.format("%s:%s:%d", mirrorId, topicId, partition);
    }

    public static void validate(String key) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Mirror key cannot be empty");
        }

        String[] tokens = key.split(":");
        if (tokens.length != 3) {
            throw new IllegalArgumentException("Invalid key format: expected - mirrorId:topicId:partition, found -  " + key);
        }

        try {
            Uuid.fromString(tokens[0].trim());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid mirror ID: " + tokens[0], e);
        }

        try {
            Uuid.fromString(tokens[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid topic ID: " + tokens[1], e);
        }

        try {
            Integer.parseInt(tokens[2]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid partition: " + tokens[2], e);
        }
    }
}
