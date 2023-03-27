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
package org.apache.kafka.server.log.remote.storage;


import org.apache.kafka.common.Uuid;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class SimpleRemoteStorageManager implements RemoteStorageManager {

    public Path tmpdir;


    private Optional<Path> getIndexFilePath(LogSegmentData logSegmentData, RemoteStorageManager.IndexType indexType) {
        switch (indexType) {
            case OFFSET:
                return Optional.of(logSegmentData.offsetIndex());
            case PRODUCER_SNAPSHOT:
                return Optional.of(logSegmentData.producerSnapshotIndex());
            case TIMESTAMP:
                return Optional.of(logSegmentData.timeIndex());
            case TRANSACTION:
                return logSegmentData.transactionIndex();
            default:
                throw new IllegalArgumentException(String.format("index type %s does not have a file path", indexType));
        }
    }
    @Override
    public void copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata, LogSegmentData logSegmentData) throws RemoteStorageException {
        System.out.println("copyLogSegmentData:" + remoteLogSegmentMetadata + ";;" + tmpdir.toString());
        try {
            Files.copy(logSegmentData.logSegment(), tmpdir.resolve(getBlobName(remoteLogSegmentMetadata, "segment")));
            System.out.println("copy to " + tmpdir.resolve(getBlobName(remoteLogSegmentMetadata, "segment")));
            for (IndexType indexType : IndexType.values()) {
                if (indexType == IndexType.LEADER_EPOCH) {
                    Files.write(tmpdir.resolve(getBlobName(remoteLogSegmentMetadata, indexType.toString())), logSegmentData.leaderEpochIndex().array());
                } else {
                    Optional<Path> path = getIndexFilePath(logSegmentData, indexType);
                    if (path.isPresent()) {
                        Files.copy(path.get(), tmpdir.resolve(getBlobName(remoteLogSegmentMetadata, indexType.toString())));
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private String getBlobName(RemoteLogSegmentMetadata remoteLogSegmentMetadata, String suffix) {
        // Azure blob name requirements
        // https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#blob-names
        int partition = remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition().partition();
        // kafka.common.Uuid.toString() uses Base64 encoding, which may contain '/' and '+'. They are valid in blob names.
        // However, we use canonical UUID naming for simplicity.
        Uuid id = remoteLogSegmentMetadata.remoteLogSegmentId().id();
        String logSegmentId = new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits()).toString();
        return String.format("%d.%s.%s", partition, logSegmentId, suffix);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition) throws RemoteStorageException {
        return fetchLogSegment(remoteLogSegmentMetadata, startPosition, Integer.MAX_VALUE);

    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition, int endPosition) throws RemoteStorageException {
        String name = getBlobName(remoteLogSegmentMetadata, "segment");
        Path path = tmpdir.resolve(name);
        try {
            byte[] content = Files.readAllBytes(path);

            int length = Math.min(content.length - 1, endPosition) - startPosition + 1;
            return new ByteArrayInputStream(content, startPosition, length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

        @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata, IndexType indexType) throws RemoteStorageException {
        String name = getBlobName(remoteLogSegmentMetadata, indexType.toString());
        Path path = tmpdir.resolve(name);
        try {
            byte[] content = Files.readAllBytes(path);

            return new ByteArrayInputStream(content);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        System.out.println("!!! deleteLogSegmentData:" + remoteLogSegmentMetadata);
        String name = getBlobName(remoteLogSegmentMetadata, "segment");
        Path path = tmpdir.resolve(name);
        try {
            Files.deleteIfExists(path);
            for (IndexType indexType : IndexType.values()) {
                name = getBlobName(remoteLogSegmentMetadata, indexType.toString());
                path = tmpdir.resolve(name);
                Files.deleteIfExists(path);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            String fileName = "/tmp/remote";

            tmpdir = Paths.get(fileName);
            if (!Files.exists(tmpdir)) {

                Files.createDirectory(tmpdir);
                System.out.println("Directory created:" + tmpdir);
            } else {

                System.out.println("Directory already exists");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
