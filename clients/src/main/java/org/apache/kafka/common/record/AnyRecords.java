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
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Records} implementation backed by a file. An optional start and end position can be applied to this
 * instance to enable slicing a range of the log records.
 */
public class AnyRecords extends FileRecords implements Closeable {
    private final boolean isSlice;
    private final int start;
    private final int end;

    private final Iterable<FileChannelRecordBatch> batches;

    // mutable state
    private AtomicInteger size;
    private  FileChannel channel;
    private volatile File file;
    private volatile Map<Long, File> file2;
    private long baseOffset = 0;
    private String path;
    private String suffix;
    private long currentOffset;
    private boolean changed = true;
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
               long baseOffset,
               String path,
               String suffix,
               Map<Long, File> file2,
               long currentOffset,
               AtomicInteger size,
               ByteBuffer recordBuffer) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = 0;
        this.end = end;
        this.isSlice = isSlice;
        this.size =size;
        this.baseOffset = baseOffset;
        this.path = path;
        this.suffix = suffix;
        this.file2 = file2;
        this.currentOffset = currentOffset;
        batches = batchesFrom(start);
        this.recordBuffer = recordBuffer;
    }

    AnyRecords(File file,
               FileChannel channel,
               int start,
               int end,
               boolean isSlice,
               long baseOffset,
               String path,
               String suffix) throws IOException {
        this(file, channel, start, end, isSlice, baseOffset, path, suffix, new HashMap<>(), -1, null, null);
    }

    AnyRecords(File file,
               FileChannel channel,
               int start,
               int end,
               boolean isSlice,
               ByteBuffer byteBuffer) throws IOException {
        this(file, channel, start, end, isSlice, 0, null, null, new HashMap<>(), -1, null, byteBuffer);
    }



    @Override
    public int sizeInBytes() {
        return recordBuffer == null ? 0 : recordBuffer.limit();
    }

    /**
     * Get the underlying file.
     * @return The file
     */
    public File file() {
        return file;
    }

    /**
     * Get the underlying file channel.
     * @return The file channel
     */
    public FileChannel channel() {
        return channel;
    }
    public ByteBuffer recordBuffer() {
        return recordBuffer;
    }

    /**
     * Read log batches into the given buffer until there are no bytes remaining in the buffer or the end of the file
     * is reached.
     *
     * @param buffer The buffer to write the batches to
     * @param position Position in the buffer to read from
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)} for details on the
     * possible exceptions
     */
    public void readInto(ByteBuffer buffer, int position) throws IOException {
        Utils.readFully(channel, buffer, position + this.start);
        buffer.flip();
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
        System.out.println("!!! slice:" + position + ";;" + size + ";;" + sizeInBytes() + ";;" + availableBytes);
        int startPosition = this.start + position;
        return new AnyRecords(file, channel, startPosition, startPosition + availableBytes, true, recordBuffer.duplicate());
    }

    /**
     * Return a slice of records from this instance, the difference with {@link AnyRecords#slice(int, int)} is
     * that the position is not necessarily on an offset boundary.
     *
     * This method is reserved for cases where offset alignment is not necessary, such as in the replication of raft
     * snapshots.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return A unaligned slice of records on this message set limited based on the given position and size
     */
    public UnalignedFileRecords sliceUnaligned(int position, int size) {
        int availableBytes = availableBytes(position, size);
        return new UnalignedFileRecords(channel, this.start + position, availableBytes);
    }

    private int availableBytes(int position, int size) {
        // Cache current size in case concurrent write changes it
        int currentSizeInBytes = sizeInBytes();

        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position + " in read from " + this);
        if (position > currentSizeInBytes - start)
            throw new IllegalArgumentException("Slice from position " + position + " exceeds end position of " + this);
        if (size < 0)
            throw new IllegalArgumentException("Invalid size: " + size + " in read from " + this);

        int end = this.start + position + size;
        // Handle integer overflow or if end is beyond the end of the file
        if (end < 0 || end > start + currentSizeInBytes)
            end = this.start + currentSizeInBytes;
        return end - (this.start + position);
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
     * Commit all written data to the physical disk
     */
    public void flush() throws IOException {
        //channel.force(true);
    }

    /**
     * Close this record set
     */
    public void close() throws IOException {
        //flush();
        //trim();
        if (channel != null)
            channel.close();
    }

    /**
     * Close file handlers used by the FileChannel but don't write to disk. This is used when the disk may have failed
     */
    public void closeHandlers() throws IOException {
        if (channel != null)
            channel.close();
    }

    /**
     * Delete this message set from the filesystem
     * @throws IOException if deletion fails due to an I/O error
     * @return  {@code true} if the file was deleted by this method; {@code false} if the file could not be deleted
     *          because it did not exist
     */
    public boolean deleteIfExists() throws IOException {
        Utils.closeQuietly(channel, "FileChannel");
        return Files.deleteIfExists(file.toPath());
    }

    /**
     * Trim file when close or roll to next file
     */
    public void trim() throws IOException {
        truncateTo(sizeInBytes());
    }

    /**
     * Update the parent directory (to be used with caution since this does not reopen the file channel)
     * @param parentDir The new parent directory
     */
    public void updateParentDir(File parentDir) {
        if (file != null)
            this.file = new File(parentDir, file.getName());
    }

    /**
     * Rename the file that backs this message set
     * @throws IOException if rename fails.
     */
    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
        } finally {
            this.file = f;
        }
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
            //channel.truncate(targetSize);
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

    /**
     * Search forward for the file position of the message batch whose last offset that is greater
     * than or equal to the target offset. If no such batch is found, return null.
     *
     * @param targetOffset     The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     * @return the batch's base offset, its physical position, and its size (including log overhead)
     */
    public FileRecords.LogOffsetPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
        for (FileChannelRecordBatch batch : batchesFrom(startingPosition)) {
            long offset = batch.lastOffset();
            System.out.println("offset:" + offset + " batch:" + batch + " batch:" + sizeInBytes());
            if (offset >= targetOffset)
                return new FileRecords.LogOffsetPosition(batch.baseOffset(), batch.position(), sizeInBytes());
        }
        return new FileRecords.LogOffsetPosition(0, 0, 0);
    }

    /**
     * Search forward for the first message that meets the following requirements:
     * - Message's timestamp is greater than or equals to the targetTimestamp.
     * - Message's position in the log file is greater than or equals to the startingPosition.
     * - Message's offset is greater than or equals to the startingOffset.
     *
     * @param targetTimestamp The timestamp to search for.
     * @param startingPosition The starting position to search.
     * @param startingOffset The starting offset to search.
     * @return The timestamp and offset of the message found. Null if no message is found.
     */
    public FileRecords.TimestampAndOffset searchForTimestamp(long targetTimestamp, int startingPosition, long startingOffset) {
        for (RecordBatch batch : batchesFrom(startingPosition)) {
            if (batch.maxTimestamp() >= targetTimestamp) {
                // We found a message
                for (Record record : batch) {
                    long timestamp = record.timestamp();
                    if (timestamp >= targetTimestamp && record.offset() >= startingOffset)
                        return new FileRecords.TimestampAndOffset(timestamp, record.offset(),
                                maybeLeaderEpoch(batch.partitionLeaderEpoch()));
                }
            }
        }
        return null;
    }

    /**
     * Return the largest timestamp of the messages after a given position in this file message set.
     * @param startingPosition The starting position.
     * @return The largest timestamp of the messages after the given position.
     */
    public FileRecords.TimestampAndOffset largestTimestampAfter(int startingPosition) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long shallowOffsetOfMaxTimestamp = -1L;
        int leaderEpochOfMaxTimestamp = RecordBatch.NO_PARTITION_LEADER_EPOCH;

        for (RecordBatch batch : batchesFrom(startingPosition)) {
            long timestamp = batch.maxTimestamp();
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
                shallowOffsetOfMaxTimestamp = batch.lastOffset();
                leaderEpochOfMaxTimestamp = batch.partitionLeaderEpoch();
            }
        }
        return new FileRecords.TimestampAndOffset(maxTimestamp, shallowOffsetOfMaxTimestamp,
                maybeLeaderEpoch(leaderEpochOfMaxTimestamp));
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                Optional.empty() : Optional.of(leaderEpoch);
    }

    /**
     * Get an iterator over the record batches in the file. Note that the batches are
     * backed by the open file channel. When the channel is closed (i.e. when this instance
     * is closed), the batches will generally no longer be readable.
     * @return An iterator over the batches
     */
    @Override
    public Iterable<FileChannelRecordBatch> batches() {
        return batches;
    }

    @Override
    public String toString() {
        return "FileRecords(size=" + sizeInBytes() +
                ", file=" + file +
                ", start=" + start +
                ", end=" + end +
                ")";
    }

    /**
     * Get an iterator over the record batches in the file, starting at a specific position. This is similar to
     * {@link #batches()} except that callers specify a particular position to start reading the batches from. This
     * method must be used with caution: the start position passed in must be a known start of a batch.
     * @param start The position to start record iteration from; must be a known position for start of a batch
     * @return An iterator over batches starting from {@code start}
     */
    public Iterable<FileChannelRecordBatch> batchesFrom(final int start) {
        return () -> batchIterator(start);
    }


    public AbstractIterator<FileChannelRecordBatch> batchIterator() {
        return batchIterator(start);
    }

    private AbstractIterator<FileChannelRecordBatch> batchIterator(long start) {
        final int end;
        if (isSlice)
            end = this.end;
        else
            end = this.sizeInBytes();
        FileLogInputStream inputStream = new FileLogInputStream(this, (int)start, end);
        return new RecordBatchIterator<>(inputStream);
    }


    public static AnyRecords open(File file,
                                  boolean mutable,
                                  boolean fileAlreadyExists,
                                  int initFileSize,
                                  boolean preallocate,
                                  long baseOffset,
                                  String path,
                                  String suffix) throws IOException {
//        FileChannel channel = openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);
        int end = Integer.MAX_VALUE;
        return new AnyRecords(null, null, 0, end, false, baseOffset, path, suffix);
    }

    public static AnyRecords open(File file,
                                  boolean fileAlreadyExists,
                                  int initFileSize,
                                  boolean preallocate,
                                  long baseOffset,
                                  String path,
                                  String suffix) throws IOException {
        return open(file, true, fileAlreadyExists, initFileSize, preallocate, baseOffset, path, suffix);
    }

    public static AnyRecords open(File file, boolean mutable) throws IOException {
        return open(file, mutable, false, 0, false, 0, "", "");
    }

    public static AnyRecords open(File file) throws IOException {
        return open(file, true);
    }


}
