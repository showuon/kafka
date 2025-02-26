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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Base interface for accessing records which could be contained in the log, or an in-memory materialization of log records.
 */
public class InMemoryStorageManager implements StorageManager {
    public int initRecords(File file,
                     boolean mutable,
                     boolean fileAlreadyExists,
                     int initFileSize,
                     boolean preallocate,
                     boolean isSlice,
                           int start,
                           int end) {
        return 0;
    }
    public void readRecords(String path, ByteBuffer buffer, int position) {}
    public int appendRecords(String path, ByteBuffer buffer) {
        return 0;
    }

    public long recordsSize(String path) throws IOException {
        return 0;
    }

    public long writeRecordsToSocket(String path, SocketChannel socketChannel, long position, long count) {
        return 0;
    }
    public void flushRecords(String path) {}
    public void closeRecords(String path) {}

    public boolean deleteIfExists(String path) {
        return false;
    }
    public void truncate(String path, int targetSize) {

    }
}
