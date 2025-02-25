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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.utils.AbstractIterator;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Records} implementation backed by a file. An optional start and end position can be applied to this
 * instance to enable slicing a range of the log records.
 */
public class AnyRecords extends FileRecords implements Closeable {
    private final boolean isSlice;
    private final int start;
    private final int end;

//    private final Iterable<FileChannelRecordBatch> batches;

    // mutable state
    private AtomicInteger size;
    private  FileChannel channel;
    private volatile File file;
    private ByteBuffer recordBuffer;

    /**
     * The {@code FileRecords.open} methods should be used instead of this constructor whenever possible.
     * The constructor is visible for tests.
     */
    AnyRecords(File file,
               FileChannel channel,
               int start,
               int end,
               boolean isSlice,
               ByteBuffer recordBuffer) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = 0;
        this.end = end;
        this.isSlice = isSlice;
        this.size = new AtomicInteger();;
        batches = batchesFrom(start);
        this.recordBuffer = recordBuffer;
    }

    @Override
    public int sizeInBytes() {
        return recordBuffer == null ? 0 : recordBuffer.limit();
    }

    public ByteBuffer recordBuffer() {
        return recordBuffer;
    }

    /**
     * Return a slice of records from this instance, which is a view into this set starting from the given position
     * and with the given size limit.
     *
     * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
     *
     * If this message set is already sliced, the position will be taken relative to that slicing.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    public AnyRecords slice(int position, int size) throws IOException {
        int availableBytes = availableBytes(position, size);
        int startPosition = this.start + position;
        return new AnyRecords(file, channel, startPosition, startPosition + availableBytes, true, recordBuffer.duplicate());
    }


    /**
     * Append a set of records to the file. This method is not thread-safe and must be
     * protected with a lock.
     *
     * @param records The records to append
     * @return the number of bytes written to the underlying file
     */
    public int append(MemoryRecords records) throws IOException {
        if (records.sizeInBytes() > Integer.MAX_VALUE - (size == null ? 0 : size.get()))
            throw new IllegalArgumentException("Append of size " + records.sizeInBytes() +
                    " bytes is too large for segment with current file position at " + size.get());

        recordBuffer = records.writeFullyToMemory(recordBuffer);
        return records.sizeInBytes();
    }

    /**
     * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
     * given size falls on a valid message boundary.
     * In some versions of the JDK truncating to the same size as the file message set will cause an
     * update of the files mtime, so truncate is only performed if the targetSize is smaller than the
     * size of the underlying FileChannel.
     * It is expected that no other threads will do writes to the log when this function is called.
     * @param targetSize The size to truncate to. Must be between 0 and sizeInBytes.
     * @return The number of bytes truncated off
     */
    public int truncateTo(int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0)
            throw new KafkaException("Attempt to truncate log segment " + file + " to " + targetSize + " bytes failed, " +
                    " size of this log segment is " + originalSize + " bytes.");
        if (targetSize < recordBuffer.limit()) {
            size.set(targetSize);
            recordBuffer.limit(targetSize);
        }
        return originalSize - targetSize;
    }

    @Override
    public int writeTo(TransferableChannel destChannel, int offset, int length) throws IOException {
        long newSize = Math.min(recordBuffer.capacity(), end) - start;
        int oldSize = sizeInBytes();
        if (newSize < oldSize)
            throw new KafkaException(String.format(
                    "Size of FileRecords %s has been truncated during write: old size %d, new size %d",
                    file.getAbsolutePath(), oldSize, newSize));

        long position = start + offset;
        int count = Math.min(length, oldSize - offset);

        return (int) destChannel.transferFrom(null, position, count, recordBuffer);
    }
}
