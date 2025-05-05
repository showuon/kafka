package org.apache.kafka.common.storage;/*
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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MinioStorageManager implements StorageManager {
    private Map<String, ByteBuffer> recordBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> indexBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> timeIndexBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> txnBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> snapshotBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> partitionMetadataBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> checkpointBufferMap = new ConcurrentHashMap<>();
    private S3Client s3Client;

    public MinioStorageManager() {
        initS3();
    }

    private void initS3() {
        String accessKey = "minioadmin";
        String secretKey = "minioadmin";
        AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        try {
            s3Client = S3Client.builder()
                    .region(Region.US_EAST_1)
                    .endpointOverride(new URI("http://localhost:9000"))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .forcePathStyle(true)
                    .build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean sharedStorage() {
        return true;
    }

    private Map<String, ByteBuffer> indexBuffer(String path) {
        if (path.contains(".timeindex")) {
            return timeIndexBufferMap;
        } else if (path.contains(".index")) {
            return indexBufferMap;
        }

        throw new IllegalArgumentException("not a correct index path:" + path);
    }

    private String key(String path, long offset, TopicIdPartition topicIdPartition) {
        return path.substring(path.lastIndexOf("/") + 1) + offset + topicIdPartition.topicId();
    }

    private String key(String path, TopicIdPartition topicIdPartition) {
        return key(path, 0, topicIdPartition);
    }

    // ----- common -------
    public boolean deleteIfExists(String path, TopicIdPartition topicIdPartition, ObjectType objectType) throws IOException {
        switch (objectType) {
            case LOG:
                recordBufferMap.remove(key(path, topicIdPartition));
                break;
            case INDEX:
                indexBuffer(path).remove(key(path, topicIdPartition));
                break;
            case TXN:
                txnBufferMap.remove(key(path, topicIdPartition));
                break;
            case SNAPSHOT:
                snapshotBufferMap.remove(key(path, topicIdPartition));
                break;
            case METADATA:
                partitionMetadataBufferMap.remove(key(path, topicIdPartition));
                break;
        }
        return true;
    }

    // no-op
    public void updateParentDir(String path, TopicIdPartition topicIdPartition, File parentDir, ObjectType objectType) {}
    public void renameTo(String path, TopicIdPartition topicIdPartition, File f, ObjectType objectType) throws IOException {}

    public int append(String path, TopicIdPartition topicIdPartition, ByteBuffer buffer, ObjectType objectType) throws IOException {
        switch (objectType) {
            case LOG:
                //return appendToBuffer(key(path, topicIdPartition), recordBufferMap, buffer);

                PutObjectRequest objectRequest = PutObjectRequest.builder()
                        .bucket("test")
                        .key(key(path, topicIdPartition))
                        .build();
                PutObjectResponse response = s3Client.putObject(objectRequest, RequestBody.fromByteBuffer(buffer));
                break;
            case TXN:
                return appendToBuffer(key(path, topicIdPartition), txnBufferMap, buffer);
            case INDEX:
                int sizeToAppend = buffer.remaining();
                indexBuffer(path).get(key(path, topicIdPartition)).put(buffer);
                return sizeToAppend;
            case SNAPSHOT:
                return appendToBuffer(key(path, topicIdPartition), snapshotBufferMap, buffer);
            case METADATA:
                return appendToBuffer(key(path, topicIdPartition), partitionMetadataBufferMap, buffer);
            case CHECKPOINT:
                return appendToBuffer(key(path, topicIdPartition), checkpointBufferMap, buffer);
        }
        return 0;
    }

    public void read(String path, TopicIdPartition topicIdPartition, ByteBuffer buffer, int position, ObjectType objectType) throws IOException {
        switch (objectType) {
            case LOG:
                buffer.put(recordBufferMap.get(key(path, topicIdPartition)).array(), position, buffer.remaining());
                break;
            case TXN:
                buffer.put(txnBufferMap.get(key(path, topicIdPartition)).array(), position, buffer.remaining());
                break;
            case SNAPSHOT:
                buffer.put(snapshotBufferMap.get(key(path, topicIdPartition)).array(), position, buffer.remaining());
                break;
            case METADATA:
                if (partitionMetadataBufferMap.containsKey(key(path, topicIdPartition))) {
                    buffer.put(partitionMetadataBufferMap.get(key(path, topicIdPartition)).array(), position, buffer.remaining());
                }
                break;
            case CHECKPOINT:
                if (checkpointBufferMap.containsKey(key(path, topicIdPartition))) {
                    buffer.put(checkpointBufferMap.get(key(path, topicIdPartition)).array(), position, buffer.remaining());
                }
                break;
        }
    }

    private int appendToBuffer(String key, Map<String, ByteBuffer> bufferMap, ByteBuffer buffer) throws IOException {
        int sizeToAppend = buffer.remaining();
        if (!bufferMap.containsKey(key)) {
            ByteBuffer temp = ByteBuffer.allocate(sizeToAppend);
            temp.put(buffer);
            bufferMap.put(key, temp);
        } else {
            ByteBuffer existingBuffer = bufferMap.get(key);
            int limit = existingBuffer.limit();
            ByteBuffer temp = ByteBuffer.allocate(limit + sizeToAppend);
            temp.put(existingBuffer.array(), 0, limit);
            temp.put(buffer);
            bufferMap.put(key, temp);
        }
        return sizeToAppend;
    }

    public long position(String path, TopicIdPartition topicIdPartition, ObjectType objectType) throws IOException {
        switch (objectType) {
            case INDEX:
                return indexBuffer(path).get(key(path, topicIdPartition)).position();
            case TXN:
                return txnBufferMap.get(key(path, topicIdPartition)).position();
            case SNAPSHOT:
                return snapshotBufferMap.get(key(path, topicIdPartition)).position();
            case METADATA:
                return partitionMetadataBufferMap.get(key(path, topicIdPartition)).position();
        }
        return 0;
    }

    public long size(String path, TopicIdPartition topicIdPartition, ObjectType objectType) throws IOException {
        if (!exist(path, topicIdPartition, objectType)) {
            return 0;
        }
        switch (objectType) {
            case LOG:
                return recordBufferMap.get(key(path, topicIdPartition)).limit();
            case INDEX:
                return indexBuffer(path).get(key(path, topicIdPartition)).limit();
            case SNAPSHOT:
                return snapshotBufferMap.get(key(path, topicIdPartition)).limit();
            case METADATA:
                return partitionMetadataBufferMap.get(key(path, topicIdPartition)).limit();
        }
        return 0;
    }

    public boolean exist(String path, TopicIdPartition topicIdPartition, ObjectType objectType) {
        switch (objectType) {
            case LOG:
                return recordBufferMap.containsKey(key(path, topicIdPartition));
            case TXN:
                return txnBufferMap.containsKey(key(path, topicIdPartition));
            case METADATA:
                return partitionMetadataBufferMap.containsKey(key(path, topicIdPartition));
        }
        return true;
    }

    public void truncate(String path, TopicIdPartition topicIdPartition, int newPos, ObjectType objectType) throws IOException {
        switch (objectType) {
            case LOG:
                recordBufferMap.get(key(path, topicIdPartition)).limit(newPos);
                break;
            case INDEX:
                indexBuffer(path).get(key(path, topicIdPartition)).limit(newPos);
                break;
            case TXN:
                txnBufferMap.get(key(path, topicIdPartition)).limit(newPos);
                break;
        }
    }

    // no-op
    public List<File> listFiles(File dir, TopicIdPartition topicIdPartition, ObjectType objectType) throws IOException {
        return Collections.emptyList();
    }







    // ----- index file ---------
    public int initRecords(File file,
                           TopicIdPartition topicIdPartition,
                     boolean mutable,
                     boolean fileAlreadyExists,
                     int initFileSize,
                     boolean preallocate,
                     boolean isSlice,
                           int start,
                           int end) {
        int size = 0;
        if (isSlice) {
            // don't check the file size if this is just a slice view
            size = end - start;
        } else {
            if (recordBufferMap.containsKey(key(file.getAbsolutePath(), topicIdPartition))) {
                ByteBuffer buffer = recordBufferMap.get(key(file.getAbsolutePath(), topicIdPartition));
                if (buffer.limit() > Integer.MAX_VALUE) {
                    throw new KafkaException("The size of segment " + file + " (" + buffer.limit() +
                            ") is larger than the maximum allowed segment size of " + Integer.MAX_VALUE);
                }

                int limit = Math.min(buffer.limit(), end);
                size = limit - start;

                // if this is not a slice, update the file pointer to the end of the file
                // set the file position to the last byte in the file
                buffer.position(limit);
            }
        }
        return size;
    }

    public long writeRecordsToSocket(String path, TopicIdPartition topicIdPartition, SocketChannel socketChannel, long position, long count) throws IOException {
        ByteBuffer buffer = recordBufferMap.get(key(path, topicIdPartition));
        buffer.position((int) position);
        buffer.slice();
        return socketChannel.write(buffer);
    }

    public void close(String path, TopicIdPartition topicIdPartition, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                recordBufferMap.remove(key(path, topicIdPartition));
                break;
            case INDEX:
                indexBuffer(path).remove(key(path, topicIdPartition));
                break;
            case TXN:
                txnBufferMap.remove(key(path, topicIdPartition));
                break;
        }
    }

    public int appendLog(String path, TopicIdPartition topicIdPartition, long offset, ByteBuffer buffer) throws IOException {
        int sizeToAppend = buffer.remaining();
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket("test")
                .key(key(path, offset, topicIdPartition))
                .build();
        PutObjectResponse response = s3Client.putObject(objectRequest, RequestBody.fromByteBuffer(buffer));
        return sizeToAppend;
    }

// GetObjectRequest objectRequest = GetObjectRequest.builder()
//                .bucket("test")
//                .key(path + "/" + baseOffset + ".log" + suffix)
//                .build();





    // ----- index file ---------
    public long initIndex(File file, TopicIdPartition topicIdPartition, int maxIndexSize, boolean writable, int entrySize) throws IOException {
        int length = roundDownToExactMultiple(maxIndexSize, entrySize);
        indexBuffer(file.getAbsolutePath()).putIfAbsent(key(file.getAbsolutePath(), topicIdPartition), ByteBuffer.allocate(length));
        return length;
    }

    public ByteBuffer indexBuffer(String path, TopicIdPartition topicIdPartition) {
        return indexBuffer(path).get(key(path, topicIdPartition)).duplicate();
    }

    public boolean resizeIndex(String path, TopicIdPartition topicIdPartition, int newSize, AtomicLong length, AtomicInteger maxEntries, int entrySize) throws IOException {
        ByteBuffer buffer = indexBuffer(path).get(key(path, topicIdPartition));
        int position = buffer.position();
        length.set(newSize);
        buffer.limit(newSize);
        maxEntries.set(buffer.limit() / entrySize);
        buffer.position(position);
        return true;
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundDownToExactMultiple(67, 8) == 64
     */
    private static int roundDownToExactMultiple(int number, int factor) {
        return factor * (number / factor);
    }

    // ----- transaction index file --------
}
