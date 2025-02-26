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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Base interface for accessing records which could be contained in the log, or an in-memory materialization of log records.
 */
public class FileStorageManager implements StorageManager {
    private Map<String, FileChannel> channelMap = new HashMap<>();
    private File file;

    public int initRecords(File file,
                     boolean mutable,
                     boolean fileAlreadyExists,
                     int initFileSize,
                     boolean preallocate,
                     boolean isSlice,
                     int start,
                     int end) throws IOException {
        this.file = file;
        FileChannel channel = openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);

        this.channelMap.put(file.getAbsolutePath(), channel);

        int size = 0;
        if (isSlice) {
            // don't check the file size if this is just a slice view
            size = end - start;
        } else {
            if (channel.size() > Integer.MAX_VALUE)
                throw new KafkaException("The size of segment " + file + " (" + channel.size() +
                        ") is larger than the maximum allowed segment size of " + Integer.MAX_VALUE);

            int limit = Math.min((int) channel.size(), end);
            size = limit - start;

            // if this is not a slice, update the file pointer to the end of the file
            // set the file position to the last byte in the file
            channel.position(limit);
        }

        return size;
    }

    /**
     * Open a channel for the given file
     * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
     * with one value (for example 512 * 1025 *1024 ) can improve the kafka produce performance.
     * @param file File path
     * @param mutable mutable
     * @param fileAlreadyExists File already exists or not
     * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
     * @param preallocate Pre-allocate file or not, gotten from configuration.
     */
    private FileChannel openChannel(File file,
                                           boolean mutable,
                                           boolean fileAlreadyExists,
                                           int initFileSize,
                                           boolean preallocate) throws IOException {
        if (mutable) {
            if (fileAlreadyExists || !preallocate) {
                return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
            } else {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(initFileSize);
                return randomAccessFile.getChannel();
            }
        } else {
            return FileChannel.open(file.toPath());
        }
    }

    public void readRecords(String path, ByteBuffer buffer, int position) throws IOException {
        Utils.readFullyOrFail(channelMap.get(path), buffer, position, "log header");
    }
    public int appendRecords(String path, ByteBuffer buffer) throws IOException {
        return channelMap.get(path).write(buffer);
    }
    public long recordsSize(String path) throws IOException {
        return channelMap.get(path).size();
    }

    public long writeRecordsToSocket(String path, SocketChannel socketChannel, long position, long count) throws IOException {
        return channelMap.get(path).transferTo(position, count, socketChannel);
    }

    public void flushRecords(String path) throws IOException {
        channelMap.get(path).force(true);
    }
    public void closeRecords(String path) throws IOException {
        channelMap.get(path).close();
    }

    public boolean deleteIfExists(String path) throws IOException {
        Utils.closeQuietly(channelMap.get(path), "FileChannel");
        return Files.deleteIfExists(file.toPath());
    }

    public void updateParentDir(String path, File parentDir) {
        if (file != null)
            this.file = new File(parentDir, file.getName());
    }
    public void renameTo(String path, File f) throws IOException {
        if (file != null) {
            try {
                Utils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
            } finally {
                this.file = f;
            }
        }
    }

    public void truncate(String path, int targetSize) throws IOException {
        channelMap.get(path).truncate(targetSize);
    }
}
