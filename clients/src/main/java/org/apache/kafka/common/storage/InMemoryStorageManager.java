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
import org.apache.kafka.common.Uuid;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemoryStorageManager implements StorageManager {
    private Map<Uuid, ByteBuffer> recordBufferMap = new ConcurrentHashMap<>();
    private Map<Uuid, ByteBuffer> indexBufferMap = new ConcurrentHashMap<>();
    private Map<Uuid, ByteBuffer> txnBufferMap = new ConcurrentHashMap<>();
    private Map<Uuid, ByteBuffer> snapshotBufferMap = new ConcurrentHashMap<>();
    private Map<Uuid, ByteBuffer> partitionMetadataBufferMap = new ConcurrentHashMap<>();
    private Map<Uuid, ByteBuffer> checkpointBufferMap = new ConcurrentHashMap<>();

    // ----- common -------
    public boolean deleteIfExists(String path, TopicIdPartition topicIdPartition, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                recordBufferMap.remove(topicIdPartition.topicId());
                break;
            case INDEX:
                indexBufferMap.remove(topicIdPartition.topicId());
                break;
            case TXN:
                txnBufferMap.remove(topicIdPartition.topicId());
                break;
            case SNAPSHOT:
                snapshotBufferMap.remove(topicIdPartition.topicId());
                break;
            case METADATA:
                partitionMetadataBufferMap.remove(topicIdPartition.topicId());
                break;
        }
        return true;
    }

    // no-op
    public void updateParentDir(String path, TopicIdPartition topicIdPartition, File parentDir, ObjectType ObjectType) {}
    public void renameTo(String path, TopicIdPartition topicIdPartition, File f, ObjectType ObjectType) throws IOException {}

    public int append(String path, TopicIdPartition topicIdPartition, ByteBuffer buffer, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                return appendToBuffer(topicIdPartition.topicId(), recordBufferMap, buffer);
            case TXN:
                return appendToBuffer(topicIdPartition.topicId(), txnBufferMap, buffer);
            case INDEX:
                int sizeToAppend = buffer.remaining();
                indexBufferMap.get(topicIdPartition.topicId()).put(buffer);
                return sizeToAppend;
            case SNAPSHOT:
                return appendToBuffer(topicIdPartition.topicId(), snapshotBufferMap, buffer);
            case METADATA:
                return appendToBuffer(topicIdPartition.topicId(), partitionMetadataBufferMap, buffer);
            case CHECKPOINT:
                return appendToBuffer(topicIdPartition.topicId(), checkpointBufferMap, buffer);
        }
        return 0;
    }

    public void read(String path, TopicIdPartition topicIdPartition, ByteBuffer buffer, int position, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                buffer.put(recordBufferMap.get(topicIdPartition.topicId()).array(), position, buffer.remaining());
                break;
            case TXN:
                buffer.put(txnBufferMap.get(topicIdPartition.topicId()).array(), position, buffer.remaining());
                break;
            case SNAPSHOT:
                buffer.put(snapshotBufferMap.get(topicIdPartition.topicId()).array(), position, buffer.remaining());
                break;
            case METADATA:
                if (partitionMetadataBufferMap.containsKey(topicIdPartition.topicId())) {
                    buffer.put(partitionMetadataBufferMap.get(topicIdPartition.topicId()).array(), position, buffer.remaining());
                }
                break;
            case CHECKPOINT:
                if (checkpointBufferMap.containsKey(topicIdPartition.topicId())) {
                    buffer.put(checkpointBufferMap.get(topicIdPartition.topicId()).array(), position, buffer.remaining());
                }
                break;
        }
    }

    private int appendToBuffer(Uuid uuid, Map<Uuid, ByteBuffer> bufferMap, ByteBuffer buffer) throws IOException {
        int sizeToAppend = buffer.remaining();
        if (!bufferMap.containsKey(uuid)) {
            ByteBuffer temp = ByteBuffer.allocate(sizeToAppend);
            temp.put(buffer);
            bufferMap.put(uuid, temp);
        } else {
            ByteBuffer existingBuffer = bufferMap.get(uuid);
            int limit = existingBuffer.limit();
            ByteBuffer temp = ByteBuffer.allocate(limit + sizeToAppend);
            temp.put(existingBuffer.array(), 0, limit);
            temp.put(buffer);
            bufferMap.put(uuid, temp);
        }
        return sizeToAppend;
    }

    public long position(String path, TopicIdPartition topicIdPartition, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case INDEX:
                return indexBufferMap.get(topicIdPartition.topicId()).position();
            case TXN:
                return txnBufferMap.get(topicIdPartition.topicId()).position();
            case SNAPSHOT:
                return snapshotBufferMap.get(topicIdPartition.topicId()).position();
            case METADATA:
                return partitionMetadataBufferMap.get(topicIdPartition.topicId()).position();
        }
        return 0;
    }

    public long size(String path, TopicIdPartition topicIdPartition, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                return recordBufferMap.get(topicIdPartition.topicId()).limit();
            case INDEX:
                return indexBufferMap.get(topicIdPartition.topicId()).limit();
            case SNAPSHOT:
                return snapshotBufferMap.get(topicIdPartition.topicId()).limit();
            case METADATA:
                return partitionMetadataBufferMap.get(topicIdPartition.topicId()).limit();
        }
        return 0;
    }

    public boolean exist(String path, TopicIdPartition topicIdPartition, ObjectType ObjectType) {
        switch (ObjectType) {
            case TXN:
                return txnBufferMap.containsKey(topicIdPartition.topicId());
            case METADATA:
                return partitionMetadataBufferMap.containsKey(topicIdPartition.topicId());
        }
        return true;
    }

    public void truncate(String path, TopicIdPartition topicIdPartition, int newPos, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                recordBufferMap.get(topicIdPartition.topicId()).limit(newPos);
                break;
            case INDEX:
                indexBufferMap.get(topicIdPartition.topicId()).limit(newPos);
                break;
            case TXN:
                txnBufferMap.get(topicIdPartition.topicId()).limit(newPos);
                break;
        }
    }

    // no-op
    public List<File> listFiles(File dir, TopicIdPartition topicIdPartition, ObjectType ObjectType) throws IOException {
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
            if (recordBufferMap.containsKey(file.getAbsolutePath())) {
                ByteBuffer buffer = recordBufferMap.get(file.getAbsolutePath());
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
        ByteBuffer buffer = recordBufferMap.get(path);
        buffer.position((int) position);
        buffer.slice();
        return socketChannel.write(buffer);
    }

    public void close(String path, TopicIdPartition topicIdPartition, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                recordBufferMap.remove(path);
                break;
            case INDEX:
                indexBufferMap.remove(path);
                break;
            case TXN:
                txnBufferMap.remove(path);
                break;
        }
    }






    // ----- index file ---------
    public long initIndex(File file, TopicIdPartition topicIdPartition, int maxIndexSize, boolean writable, int entrySize) throws IOException {
        int length = roundDownToExactMultiple(maxIndexSize, entrySize);
        indexBufferMap.putIfAbsent(topicIdPartition.topicId(), ByteBuffer.allocate(length));
        return length;
    }

    public ByteBuffer indexBuffer(String path, TopicIdPartition topicIdPartition) {
        return indexBufferMap.get(path).duplicate();
    }

    public boolean resizeIndex(String path, TopicIdPartition topicIdPartition, int newSize, AtomicLong length, AtomicInteger maxEntries, int entrySize) throws IOException {
        ByteBuffer buffer = indexBufferMap.get(path);
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
