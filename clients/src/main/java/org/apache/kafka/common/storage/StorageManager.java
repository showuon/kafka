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
package org.apache.kafka.common.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The interface for accessing partition logs, indexes, metadata files....
 */
public interface StorageManager {
    enum StorageType {
        LOG,
        INDEX,
        TXN
    }

    // ----- common -------
    boolean deleteIfExists(String path, StorageType storageType) throws IOException;
    void updateParentDir(String path, File parentDir, StorageType storageType);
    void renameTo(String path, File f, StorageType storageType) throws IOException;
    void truncate(String path, int newPos, StorageType storageType) throws IOException;
    int append(String path, ByteBuffer buffer, StorageType storageType) throws IOException;
    void read(String path, ByteBuffer buffer, int position, StorageType storageType) throws IOException;
    default void flush(String path, StorageType storageType) throws IOException {}
    default void close(String path, StorageType storageType) throws IOException {}
    long position(String path, StorageType storageType) throws IOException;
    boolean isEmpty(String path, StorageType storageType);


    // ---- log files ----
    int initRecords(File file,
                     boolean mutable,
                     boolean fileAlreadyExists,
                     int initFileSize,
                     boolean preallocate,
                     boolean isSlice,
                    int start,
                    int end) throws IOException;

    long recordsSize(String path) throws IOException;
    long writeRecordsToSocket(String path, SocketChannel socketChannel, long position, long count) throws IOException;


    // ---- index files ----
    long initIndex(File file, int maxIndexSize, boolean writable, int entrySize) throws IOException;
    ByteBuffer indexBuffer(String path);
    boolean resizeIndex(String path, int newSize, AtomicLong length, AtomicInteger maxEntries, int entrySize) throws IOException;

    // ----- transaction index file --------
    default void initTransIndex(File file) throws IOException {}
}
