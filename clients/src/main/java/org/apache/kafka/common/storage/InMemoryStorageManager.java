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
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base interface for accessing records which could be contained in the log, or an in-memory materialization of log records.
 */
public class InMemoryStorageManager implements StorageManager {
    private Map<String, ByteBuffer> recordBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> indexBufferMap = new ConcurrentHashMap<>();

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
    public void readRecords(String path, ByteBuffer buffer, int position) {
        buffer.put(recordBufferMap.get(path).array(), position, buffer.remaining());
    }
    public int appendRecords(String path, ByteBuffer buffer) {
        int remaining = buffer.remaining();
        if (!recordBufferMap.containsKey(path)) {
            ByteBuffer temp = ByteBuffer.allocate(remaining);
            temp.put(buffer);
            recordBufferMap.put(path, temp);
        } else {
            ByteBuffer existingBuffer = recordBufferMap.get(path);
            int limit = existingBuffer.limit();
            ByteBuffer temp = ByteBuffer.allocate(limit + remaining);
            temp.put(existingBuffer.array(), 0, limit);
            temp.put(buffer);
            recordBufferMap.put(path, temp);
        }
        return remaining;
    }

    public long recordsSize(String path) throws IOException {
        return recordBufferMap.get(path).limit();
    }

    public long writeRecordsToSocket(String path, SocketChannel socketChannel, long position, long count) throws IOException {
        ByteBuffer buffer = recordBufferMap.get(path);
        buffer.position((int) position);
        buffer.slice();
        return socketChannel.write(buffer);
    }
    public void flushRecords(String path) {}
    public void closeRecords(String path) {}

    @Override
    public boolean deleteRecordsIfExists(String path) {
        recordBufferMap.remove(path);
        return true;
    }

    @Override
    public void updateRecordsParentDir(String path, File parentDir) {
        ByteBuffer buffer = recordBufferMap.get(path);
        if (buffer == null)
            return;
        File tempFile = new File(path);
        File updatedFile = new File(parentDir, tempFile.getName());
        recordBufferMap.put(updatedFile.getAbsolutePath(), buffer);
    }

    @Override
    public void renameRecordsTo(String path, File f) {
        ByteBuffer buffer = indexBufferMap.remove(path);
        indexBufferMap.put(f.getAbsolutePath(), buffer);
    }

    public void truncateRecords(String path, int targetSize) {
        recordBufferMap.get(path).limit(targetSize);
    }

    public long initIndex(File file, int maxIndexSize, boolean writable, int entrySize) throws IOException {
        int length = roundDownToExactMultiple(maxIndexSize, entrySize);
        indexBufferMap.putIfAbsent(file.getAbsolutePath(), ByteBuffer.allocate(length));
        return length;
    }

    public ByteBuffer indexBuffer(String path) {
        return indexBufferMap.get(path);
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

    public void renameIndex(String path, File f) throws IOException {
        ByteBuffer buffer = indexBufferMap.remove(path);
        indexBufferMap.put(f.getAbsolutePath(), buffer);
    }

    public void closeIndex(String path) throws IOException {
        indexBufferMap.remove(path);
    }

    public void truncateIndexEntries(String path, int newPos) {
        indexBufferMap.get(path).position(newPos);
    }

    @Override
    public void updateIndexParentDir(String path, File parentDir) {

    }
}
