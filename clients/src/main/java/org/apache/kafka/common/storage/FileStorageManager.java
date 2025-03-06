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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileStorageManager implements StorageManager {
    private final Map<String, FileChannel> channelMap = new ConcurrentHashMap<>();
    private final Map<String, MappedByteBuffer> indexBufferMap = new ConcurrentHashMap<>();
    private final Map<String, FileChannel> txnChannelMap = new ConcurrentHashMap<>();


    // ----- common -------
    public boolean deleteIfExists(String path, StorageType storageType) throws IOException {
        switch (storageType) {
            case LOG:
                FileChannel fileChannel = channelMap.remove(path);
                Utils.closeQuietly(fileChannel, "FileChannel");
                break;
            case INDEX:
                indexBufferMap.remove(path);
                break;
            case TXN:
                txnChannelMap.remove(path);
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
                channelMap.put(updatedFile.getAbsolutePath(), channelMap.remove(path));
                break;
            case INDEX:
                indexBufferMap.put(updatedFile.getAbsolutePath(), indexBufferMap.remove(path));
                break;
            case TXN:
                txnChannelMap.put(updatedFile.getAbsolutePath(), txnChannelMap.remove(path));
                break;
        }
    }

    public void renameTo(String path, File f, StorageType storageType) throws IOException {
        Utils.atomicMoveWithFallback(new File(path).toPath(), f.toPath(), false);
        switch (storageType) {
            case LOG:
                channelMap.put(f.getAbsolutePath(), channelMap.remove(path));
                break;
            case INDEX:
                indexBufferMap.put(f.getAbsolutePath(), indexBufferMap.remove(path));
                break;
            case TXN:
                txnChannelMap.put(f.getAbsolutePath(), txnChannelMap.remove(path));
                break;
        }
    }

    public int append(String path, ByteBuffer buffer, StorageType storageType) throws IOException {
        int sizeToAppend = buffer.remaining();
        switch (storageType) {
            case LOG:
                return channelMap.get(path).write(buffer);
            case TXN:
                if (!txnChannelMap.containsKey(path)) {
                    openTxnChannel(new File(path));
                }
                Utils.writeFully(txnChannelMap.get(path), buffer);
                break;
            case INDEX:
                indexBufferMap.get(path).put(buffer);
                break;
            case SNAPSHOT:
                try (FileChannel fileChannel = FileChannel.open(new File(path).toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                    Utils.writeFully(fileChannel, buffer);
                }
                break;
            case METADATA:
            case CHECKPOINT:
                File tmpFile = new File(path + ".tmp");
                try (FileChannel fileChannel = FileChannel.open(tmpFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                    Utils.writeFully(fileChannel, buffer);
                }
                Utils.atomicMoveWithFallback(tmpFile.toPath(), new File(path).toPath());
                break;
        }
        return sizeToAppend;
    }

    public void read(String path, ByteBuffer buffer, long position, StorageType storageType) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            return;
        }
        switch (storageType) {
            case LOG:
                Utils.readFully(channelMap.get(path), buffer, position);
                break;
            case TXN:
                Utils.readFully(txnChannelMap.get(path), buffer, position);
                break;
            case SNAPSHOT:
            case METADATA:
            case CHECKPOINT:
                try (FileChannel fileChannel = FileChannel.open(new File(path).toPath(), StandardOpenOption.READ)) {
                    Utils.readFully(fileChannel, buffer, position);
                }
                break;
        }
    }

    public void flush(String path, StorageType storageType) throws IOException {
        switch (storageType) {
            case LOG:
                channelMap.get(path).force(true);
                break;
            case INDEX:
                MappedByteBuffer mmap = indexBufferMap.get(path);
                if (mmap != null)
                    mmap.force();
                break;
            case TXN:
                txnChannelMap.get(path).force(true);
                break;
            case SNAPSHOT:
                try (FileChannel fileChannel = FileChannel.open(new File(path).toPath(), StandardOpenOption.READ)) {
                    fileChannel.force(true);
                }
                break;
        }
    }

    public void close(String path, StorageType storageType) throws IOException {
        switch (storageType) {
            case LOG:
                FileChannel fileChannel = channelMap.remove(path);
                fileChannel.close();
                break;
            case INDEX:
                MappedByteBuffer mmap = indexBufferMap.get(path);
                safeForceUnmap(path, mmap);
                break;
            case TXN:
                fileChannel = txnChannelMap.remove(path);
                if (fileChannel != null)
                    fileChannel.close();
                break;
        }
    }

    public long position(String path, StorageType storageType) throws IOException {
        switch (storageType) {
            case INDEX:
                return indexBufferMap.get(path).position();
            case TXN:
                return txnChannelMap.get(path).position();
        }
        return 0;
    }

    public long size(String path, StorageType storageType) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            return 0;
        }
        switch (storageType) {
            case LOG:
                return channelMap.get(path).size();
            case INDEX:
                return indexBufferMap.get(path).limit();
            case SNAPSHOT:
            case METADATA:
            case CHECKPOINT:
                try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
                    return fileChannel.size();
                }
        }
        return 0;
    }

    public boolean exist(String path, StorageType storageType) {
        File file = new File(path);
        return file.exists();
    }

    public void truncate(String path, int newPos, StorageType storageType) throws IOException {
        switch (storageType) {
            case LOG:
                channelMap.get(path).truncate(newPos);
                break;
            case INDEX:
                indexBufferMap.get(path).position(newPos);
                break;
            case TXN:
                txnChannelMap.get(path).position(newPos);
                break;
        }
    }

    public List<File> listFiles(File dir, StorageType storageType) throws IOException {
        if (dir.exists() && dir.isDirectory()) {
            switch (storageType) {
                case SNAPSHOT:
                    try (Stream<Path> paths = Files.list(dir.toPath())) {
                        return paths.filter(this::isSnapshotFile)
                                .map(Path::toFile).collect(Collectors.toList());
                    }
                case ALL:
                    return Arrays.stream(dir.listFiles()).filter(f -> f.isFile()).collect(Collectors.toList());
            }
        }
        return Collections.emptyList();
    }

    private boolean isSnapshotFile(Path path) {
        String PRODUCER_SNAPSHOT_FILE_SUFFIX = ".snapshot";
        return Files.isRegularFile(path) && path.getFileName().toString().endsWith(PRODUCER_SNAPSHOT_FILE_SUFFIX);
    }

    public long lastModified(File file) {
        return file.lastModified();
    }

    public void setLastModified(File file, long lastModified) throws IOException {
        Files.setLastModifiedTime(file.toPath(), FileTime.fromMillis(lastModified));
    }





    // ----- log file ---------
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

    public long writeRecordsToSocket(String path, SocketChannel socketChannel, long position, long count) throws IOException {
        return channelMap.get(path).transferTo(position, count, socketChannel);
    }









    // ----- index file ---------
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
        return indexBufferMap.get(path).duplicate();
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





    // ----- transaction index -------
    public void initTransIndex(File file) throws IOException {
        if (file.exists()) {
            openTxnChannel(file);
        }
    }

    private void openTxnChannel(File file) throws IOException {
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel.position(channel.size());
        txnChannelMap.put(file.getAbsolutePath(), channel);
    }

}
