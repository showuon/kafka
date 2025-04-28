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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The interface for accessing partition logs, indexes, metadata files....
 */
public interface StorageManager {
    /**
     * The storage types under partition folder
     */
    enum ObjectType {
        LOG,
        // INDEX includes offset index and time index
        INDEX,
        TXN,
        SNAPSHOT,
        CHECKPOINT,
        METADATA,
        // ALL types of files
        ALL
    }

    // ----- common -------

    /**
     * Delete the object if existed.
     */
    boolean deleteIfExists(String path, ObjectType ObjectType) throws IOException;
    /**
     * Update the parent directory. Used when partition movement, and stray logs handling, ex: Update the new created partition
     * folder as the normal partition name without "-future" suffix, and update the removed partition folder as "-deleted"...
     */
    void updateParentDir(String path, File parentDir, ObjectType ObjectType);

    /**
     * Rename the Object. Used when file deletion (i.e. adding ".delete" suffix), compacted file marking (i.e. adding ".cleaned" suffix),...etc
     */
    void renameTo(String path, File f, ObjectType ObjectType) throws IOException;
    /**
     * Truncate the object to a new position. This position could be an offset (ex: index files), a file position (ex: local file)
     */
    void truncate(String path, int newPos, ObjectType ObjectType) throws IOException;
    /**
     * Append the provided buffer to the backend storage
     */
    int append(String path, ByteBuffer buffer, ObjectType ObjectType) throws IOException;
    /**
     * Read the content from the backend storage to the provided buffer
     */
    void read(String path, ByteBuffer buffer, int position, ObjectType ObjectType) throws IOException;
    /**
     * Flush the data into the backend storage
     */
    default void flush(String path, ObjectType ObjectType) throws IOException {}
    /**
     * Close the resource used in the backend storage
     */
    default void close(String path, ObjectType ObjectType) throws IOException {}
    /**
     * Get the current position in the backend storage
     */
    long position(String path, ObjectType ObjectType) throws IOException;
    /**
     * Get the current size of the backend storage
     */
    long size(String path, ObjectType ObjectType) throws IOException;
    /**
     * Test whether if the path file existed or not in the backend storage
     */
    boolean exist(String path, ObjectType ObjectType);
    /**
     * List files/objects under a directory. Used when log loader to verify if there is any temporary files, or
     * log segment files loader to check if log recovery is needed.
     */
    List<File> listFiles(File dir, ObjectType ObjectType) throws IOException;


    // ---- log files ----
    /**
     * Initialize records used to store logs
     */
    int initRecords(File file,
                     boolean mutable,
                     boolean fileAlreadyExists,
                     int initFileSize,
                     boolean preallocate,
                     boolean isSlice,
                     int start,
                     int end) throws IOException;
    /**
     * Write the records located to the socket channel. If the backend storage supports zero-copy or other optimization,
     * it can be applied here.
     */
    long writeRecordsToSocket(String path, SocketChannel socketChannel, long position, long count) throws IOException;


    // ---- index files ----
    /**
     * Initialize the index objects
     */
    long initIndex(File file, int maxIndexSize, boolean writable, int entrySize) throws IOException;
    /**
     * Retrieve the content of the index files.
     */
    ByteBuffer indexBuffer(String path);
    /**
     * Reset the size of the index object. This is used in two kinds of cases: (1) in
     * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
     * loading segments from disk or truncating back to an old segment where a new log segment became active;
     * we want to reset the index size to maximum index size to avoid rolling new segment.
     *
     */
    boolean resizeIndex(String path, int newSize, AtomicLong length, AtomicInteger maxEntries, int entrySize) throws IOException;

    // ----- transaction index file --------
    /**
     * Initialize transaction index files
     */
    default void initTransIndex(File file) throws IOException {}
}
