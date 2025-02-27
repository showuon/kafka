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
import org.apache.kafka.common.utils.ByteBufferUnmapper;
import org.apache.kafka.common.utils.OperatingSystem;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base interface for accessing records which could be contained in the log, or an in-memory materialization of log records.
 */
public class FileStorageManager implements StorageManager {
    private Map<String, FileChannel> channelMap = new ConcurrentHashMap<>();
    private Map<String, MappedByteBuffer> indexBufferMap = new ConcurrentHashMap<>();

    public int initRecords(File file,
                     boolean mutable,
                     boolean fileAlreadyExists,
                     int initFileSize,
                     boolean preallocate,
                     boolean isSlice,
                     int start,
                     int end) throws IOException {
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

    public boolean deleteRecordsIfExists(String path) throws IOException {
        FileChannel fileChannel = channelMap.remove(path);
        Utils.closeQuietly(fileChannel, "FileChannel");
        File file = new File(path);
        return Files.deleteIfExists(file.toPath());
    }

    public void updateRecordsParentDir(String path, File parentDir) {
        FileChannel channel = channelMap.remove(path);
        File tempFile = new File(path);
        File updatedFile = new File(parentDir, tempFile.getName());
        channelMap.put(updatedFile.getAbsolutePath(), channel);
    }
    public void renameRecordsTo(String path, File f) throws IOException {
        FileChannel channel = channelMap.remove(path);
        channelMap.put(f.getAbsolutePath(), channel);
    }

    public void truncateRecords(String path, int targetSize) throws IOException {
        channelMap.get(path).truncate(targetSize);
    }

    public long initIndex(File file, int maxIndexSize, boolean writable, int entrySize) throws IOException {
        boolean newlyCreated = file.createNewFile();
        RandomAccessFile raf;
        if (writable)
            raf = new RandomAccessFile(file, "rw");
        else
            raf = new RandomAccessFile(file, "r");

        try {
            /* pre-allocate the file if necessary */
            if (newlyCreated) {
                raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize));
            }

            long length = raf.length();
            MappedByteBuffer mmap = createMappedBuffer(raf, newlyCreated, length, writable, entrySize);
            indexBufferMap.put(file.getAbsolutePath(), mmap);
            return length;
        } finally {
            Utils.closeQuietly(raf, "index " + file.getName());
        }
    }

    private static MappedByteBuffer createMappedBuffer(RandomAccessFile raf, boolean newlyCreated, long length,
                                                       boolean writable, int entrySize) throws IOException {
        MappedByteBuffer idx;
        if (writable)
            idx = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, length);
        else
            idx = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length);

        /* set the position in the index for the next entry */
        if (newlyCreated)
            idx.position(0);
        else
            // if this is a pre-existing index, assume it is valid and set position to last entry
            idx.position(roundDownToExactMultiple(idx.limit(), entrySize));

        return idx;
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundDownToExactMultiple(67, 8) == 64
     */
    private static int roundDownToExactMultiple(int number, int factor) {
        return factor * (number / factor);
    }

    public ByteBuffer indexBuffer(String path) {
        return indexBufferMap.get(path);
    }

    public boolean resizeIndex(String path, int newSize, AtomicLong length, AtomicInteger maxEntries, int entrySize) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(new File(path), "rw");
        try {
            MappedByteBuffer buffer = indexBufferMap.get(path);
            int position = buffer.position();

            /* Windows or z/OS won't let us modify the file length while the file is mmapped :-( */
            if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
                safeForceUnmap(path, buffer);
            raf.setLength(newSize);
            length.set(newSize);
            buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, newSize);
            maxEntries.set(buffer.limit() / entrySize);
            buffer.position(position);
            indexBufferMap.put(path, buffer);
            return true;
        } finally {
            Utils.closeQuietly(raf, "index file " + path);
        }
    }

    protected void safeForceUnmap(String path, MappedByteBuffer buffer) throws IOException {
        if (buffer != null) {
            forceUnmap(path, buffer);
        }
    }

    /**
     * Forcefully free the buffer's mmap.
     */
    // Visible for testing
    protected void forceUnmap(String path, MappedByteBuffer buffer) throws IOException {
        try {
            ByteBufferUnmapper.unmap(path, buffer);
        } finally {
            indexBufferMap.remove(path);
        }
    }

    public void renameIndex(String path, File f) throws IOException {
        Utils.atomicMoveWithFallback(Path.of(path), f.toPath(), false);
        MappedByteBuffer buffer = indexBufferMap.remove(path);
        indexBufferMap.put(f.getAbsolutePath(), buffer);
    }

    public void flushIndex(String path) {
        MappedByteBuffer mmap = indexBufferMap.get(path);
        if (mmap != null)
            mmap.force();
    }

    public void closeIndex(String path) throws IOException {
        MappedByteBuffer mmap = indexBufferMap.get(path);
        safeForceUnmap(path, mmap);
    }

    public void truncateIndexEntries(String path, int newPos) {
        indexBufferMap.get(path).position(newPos);
    }

    public void updateIndexParentDir(String path, File parentDir) {
        File tempFile = new File(path);
        File returnFile = new File(parentDir, tempFile.getName());
        MappedByteBuffer buffer = indexBufferMap.remove(path);
        indexBufferMap.put(returnFile.getAbsolutePath(), buffer);
    }
}
