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
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryStorageManager implements StorageManager {
    private Map<String, ByteBuffer> recordBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> indexBufferMap = new ConcurrentHashMap<>();
    private Map<String, ByteBuffer> txnBufferMap = new ConcurrentHashMap<>();

    // ----- common -------
    public boolean deleteIfExists(String path, StorageType storageType) throws IOException {
        switch (storageType) {
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

        File file = new File(path);
        return Files.deleteIfExists(file.toPath());

    }

    public void updateParentDir(String path, File parentDir, StorageType storageType) {
        File existingFile = new File(path);
        File updatedFile = new File(parentDir, existingFile.getName());
        switch (storageType) {
            case LOG:
                recordBufferMap.put(updatedFile.getAbsolutePath(), recordBufferMap.remove(path));
                break;
            case INDEX:
                indexBufferMap.put(updatedFile.getAbsolutePath(), indexBufferMap.remove(path));
                break;
            case TXN:
                txnBufferMap.put(updatedFile.getAbsolutePath(), txnBufferMap.remove(path));
                break;
        }
    }

    public void renameTo(String path, File f, StorageType storageType) throws IOException {
        switch (storageType) {
            case LOG:
                recordBufferMap.put(f.getAbsolutePath(), recordBufferMap.remove(path));
                break;
            case INDEX:
                indexBufferMap.put(f.getAbsolutePath(), indexBufferMap.remove(path));
                break;
            case TXN:
                txnBufferMap.put(f.getAbsolutePath(), txnBufferMap.remove(path));
                break;
        }
    }

    public int append(String path, ByteBuffer buffer, StorageType storageType) throws IOException {
        switch (storageType) {
            case LOG:
                appendToBuffer(path, recordBufferMap, buffer);
                break;
            case TXN:
                appendToBuffer(path, txnBufferMap, buffer);
                break;
        }
        return buffer.remaining();
    }

    public void read(String path, ByteBuffer buffer, int position, StorageType storageType) throws IOException {
        switch (storageType) {
            case LOG:
                buffer.put(recordBufferMap.get(path).array(), position, buffer.remaining());
                break;
            case TXN:
                buffer.put(txnBufferMap.get(path).array(), position, buffer.remaining());
                break;
        }
    }

    private void appendToBuffer(String path, Map<String, ByteBuffer> bufferMap, ByteBuffer buffer) throws IOException {
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
    }

    public long position(String path, StorageType storageType) throws IOException {
        switch (storageType) {
            case INDEX:
                return indexBufferMap.get(path).position();
            case TXN:
                return txnBufferMap.get(path).position();
        }
        return 0;
    }

    public boolean isEmpty(String path, StorageType storageType) {
        switch (storageType) {
            case TXN:
                return txnBufferMap.containsKey(path);
        }
        return true;
    }

    public void truncate(String path, int newPos, StorageType storageType) throws IOException {
        switch (storageType) {
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

    public long recordsSize(String path) throws IOException {
        return recordBufferMap.get(path).limit();
    }

    public long writeRecordsToSocket(String path, SocketChannel socketChannel, long position, long count) throws IOException {
        ByteBuffer buffer = recordBufferMap.get(path);
        buffer.position((int) position);
        buffer.slice();
        return socketChannel.write(buffer);
    }

    public void close(String path, StorageType storageType) throws IOException {
        switch (storageType) {
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

    // ----- transaction index file --------
}
