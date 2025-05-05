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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemoryStorageManager implements StorageManager {
    private Map<String, ByteBuffer> recordBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> indexBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> txnBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> snapshotBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> partitionMetadataBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> checkpointBufferMap = new ConcurrentHashMap<>();

    // ----- common -------
    public boolean deleteIfExists(String path, ObjectType ObjectType) throws IOException {
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
            case SNAPSHOT:
                snapshotBufferMap.remove(path);
                break;
            case METADATA:
                partitionMetadataBufferMap.remove(path);
                break;
        }
        return true;
    }

    public void updateParentDir(String path, File parentDir, ObjectType ObjectType) {
        File existingFile = new File(path);
        File updatedFile = new File(parentDir, existingFile.getName());
        switch (ObjectType) {
            case LOG:
                if (recordBufferMap.containsKey(path)) {
                    recordBufferMap.put(updatedFile.getAbsolutePath(), recordBufferMap.remove(path));
                }
                break;
            case INDEX:
                if (indexBufferMap.containsKey(path)) {
                    indexBufferMap.put(updatedFile.getAbsolutePath(), indexBufferMap.remove(path));
                }
                break;
            case TXN:
                if (txnBufferMap.containsKey(path)) {
                    txnBufferMap.put(updatedFile.getAbsolutePath(), txnBufferMap.remove(path));
                }
                break;
            case SNAPSHOT:
                if (snapshotBufferMap.containsKey(path)) {
                    snapshotBufferMap.put(updatedFile.getAbsolutePath(), snapshotBufferMap.remove(path));
                }
                break;
        }
    }

    public void renameTo(String path, File f, ObjectType ObjectType) throws IOException {
        if (!exist(path, ObjectType)) {
            return;
        }
        switch (ObjectType) {
            case LOG:
                recordBufferMap.put(f.getAbsolutePath(), recordBufferMap.remove(path));
                break;
            case INDEX:
                indexBufferMap.put(f.getAbsolutePath(), indexBufferMap.remove(path));
                break;
            case TXN:
                txnBufferMap.put(f.getAbsolutePath(), txnBufferMap.remove(path));
                break;
            case SNAPSHOT:
                snapshotBufferMap.put(f.getAbsolutePath(), snapshotBufferMap.remove(path));
                break;
        }
    }

    public int append(String path, ByteBuffer buffer, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                return appendToBuffer(path, recordBufferMap, buffer);
            case TXN:
                return appendToBuffer(path, txnBufferMap, buffer);
            case INDEX:
                int sizeToAppend = buffer.remaining();
                indexBufferMap.get(path).put(buffer);
                return sizeToAppend;
            case SNAPSHOT:
                return appendToBuffer(path, snapshotBufferMap, buffer);
            case METADATA:
                return appendToBuffer(path, partitionMetadataBufferMap, buffer);
            case CHECKPOINT:
                return appendToBuffer(path, checkpointBufferMap, buffer);
        }
        return 0;
    }

    public void read(String path, ByteBuffer buffer, int position, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                buffer.put(recordBufferMap.get(path).array(), position, buffer.remaining());
                break;
            case TXN:
                buffer.put(txnBufferMap.get(path).array(), position, buffer.remaining());
                break;
            case SNAPSHOT:
                buffer.put(snapshotBufferMap.get(path).array(), position, buffer.remaining());
                break;
            case METADATA:
                if (partitionMetadataBufferMap.containsKey(path)) {
                    buffer.put(partitionMetadataBufferMap.get(path).array(), position, buffer.remaining());
                }
                break;
            case CHECKPOINT:
                if (checkpointBufferMap.containsKey(path)) {
                    buffer.put(checkpointBufferMap.get(path).array(), position, buffer.remaining());
                }
                break;
        }
    }

    private int appendToBuffer(String path, Map<String, ByteBuffer> bufferMap, ByteBuffer buffer) throws IOException {
        int sizeToAppend = buffer.remaining();
        if (!bufferMap.containsKey(path)) {
            ByteBuffer temp = ByteBuffer.allocate(sizeToAppend);
            temp.put(buffer);
            bufferMap.put(path, temp);
        } else {
            ByteBuffer existingBuffer = bufferMap.get(path);
            int limit = existingBuffer.limit();
            ByteBuffer temp = ByteBuffer.allocate(limit + sizeToAppend);
            temp.put(existingBuffer.array(), 0, limit);
            temp.put(buffer);
            bufferMap.put(path, temp);
        }
        return sizeToAppend;
    }

    public long position(String path, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case INDEX:
                return indexBufferMap.get(path).position();
            case TXN:
                return txnBufferMap.get(path).position();
            case SNAPSHOT:
                return snapshotBufferMap.get(path).position();
            case METADATA:
                return partitionMetadataBufferMap.get(path).position();
        }
        return 0;
    }

    public long size(String path, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                return recordBufferMap.get(path).limit();
            case INDEX:
                return indexBufferMap.get(path).limit();
            case SNAPSHOT:
                return snapshotBufferMap.get(path).limit();
            case METADATA:
                return partitionMetadataBufferMap.get(path).limit();
        }
        return 0;
    }

    public boolean exist(String path, ObjectType ObjectType) {
        switch (ObjectType) {
            case TXN:
                return txnBufferMap.containsKey(path);
            case METADATA:
                return partitionMetadataBufferMap.containsKey(path);
        }
        return true;
    }

    public void truncate(String path, int newPos, ObjectType ObjectType) throws IOException {
        switch (ObjectType) {
            case LOG:
                recordBufferMap.get(path).limit(newPos);
                break;
            case INDEX:
                indexBufferMap.get(path).limit(newPos);
                break;
            case TXN:
                txnBufferMap.get(path).limit(newPos);
                break;
        }
    }

    public List<File> listFiles(File dir, ObjectType ObjectType) throws IOException {
        String PRODUCER_SNAPSHOT_FILE_SUFFIX = ".snapshot";
        switch (ObjectType) {
            case SNAPSHOT:
                return snapshotBufferMap.keySet().stream().filter(path -> path.endsWith(PRODUCER_SNAPSHOT_FILE_SUFFIX))
                        .map(File::new).collect(Collectors.toList());
            case ALL:
                List<File> files = new ArrayList<>();
                files.addAll(recordBufferMap.keySet().stream().map(File::new).collect(Collectors.toList()));
                files.addAll(indexBufferMap.keySet().stream().map(File::new).collect(Collectors.toList()));
                files.addAll(txnBufferMap.keySet().stream().map(File::new).collect(Collectors.toList()));
                files.addAll(snapshotBufferMap.keySet().stream().map(File::new).collect(Collectors.toList()));
                files.addAll(partitionMetadataBufferMap.keySet().stream().map(File::new).collect(Collectors.toList()));
                files.addAll(checkpointBufferMap.keySet().stream().map(File::new).collect(Collectors.toList()));
                return files;
        }
        return Collections.emptyList();
    }







    // ----- index file ---------
    public int initRecords(File file,
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

    public long writeRecordsToSocket(String path, SocketChannel socketChannel, long position, long count) throws IOException {
        ByteBuffer buffer = recordBufferMap.get(path);
        buffer.position((int) position);
        buffer.slice();
        return socketChannel.write(buffer);
    }

    public void close(String path, ObjectType ObjectType) throws IOException {
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
    public long initIndex(File file, int maxIndexSize, boolean writable, int entrySize) throws IOException {
        int length = roundDownToExactMultiple(maxIndexSize, entrySize);
        indexBufferMap.putIfAbsent(file.getAbsolutePath(), ByteBuffer.allocate(length));
        return length;
    }

    public ByteBuffer indexBuffer(String path) {
        return indexBufferMap.get(path).duplicate();
    }

    public boolean resizeIndex(String path, int newSize, AtomicLong length, AtomicInteger maxEntries, int entrySize) throws IOException {
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
