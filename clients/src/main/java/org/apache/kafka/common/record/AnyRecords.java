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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
               AtomicInteger size) throws IOException {
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

//        if (isSlice) {
//            // don't check the file size if this is just a slice view
//            size.set(end - start);
//        } else {
//            if (channel.size() > Integer.MAX_VALUE)
//                throw new KafkaException("The size of segment " + file + " (" + channel.size() +
//                        ") is larger than the maximum allowed segment size of " + Integer.MAX_VALUE);
//
//            int limit = Math.min((int) channel.size(), end);
//            size.set(limit - start);
//
//            // if this is not a slice, update the file pointer to the end of the file
//            // set the file position to the last byte in the file
//            channel.position(limit);
//        }
//        size.set(71);

        batches = batchesFrom((long) start);
    }

    AnyRecords(File file,
               FileChannel channel,
               int start,
               int end,
               boolean isSlice,
               long baseOffset,
               String path,
               String suffix) throws IOException {
        this(file, channel, start, end, isSlice, baseOffset, path, suffix, new HashMap<>(), -1, null);
    }

    private List<String> listBucket() {
//        System.out.println("!!! list S3:");
        String accessKey = "minioadmin";
        String secretKey = "minioadmin";
        AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        S3Client s3 = null;
        try {
            s3 = S3Client.builder()
                    .region(Region.US_EAST_1)
                    .endpointOverride(new URI("http://localhost:9000"))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .forcePathStyle(true)
                    .build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                .bucket("test")
                .maxKeys(100)
                .build();


        List<String> objs = new LinkedList<>();
        ListObjectsV2Iterable objectBytes = s3.listObjectsV2Paginator(initialRequest);
        objectBytes.stream().forEach(response -> response.contents().forEach(s3Object -> {
            objs.add(s3Object.key());
        }));
        return objs;
    }


    @Override
    public int sizeInBytes() {
        if (size == null) {
            // luke

            List<String> baseOffsets = listBucket().stream().filter(name -> name.contains(path.substring(1))).sorted().collect(Collectors.toList());
            for (String baseOffset : baseOffsets) {
                long offset = Long.parseLong(baseOffset.substring(baseOffset.lastIndexOf('/') + 1, baseOffset.lastIndexOf('.')));
                readS3(offset);
            }


        }
        int res = 0;
        for (File file : file2.values()) {
            res += file.length();
        }
        System.out.println("!!! size:" + res);
        return res;
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
    public AnyRecords slice(long offset, int size) throws IOException {
//        int availableBytes = availableBytes(offset, size);
//        int startPosition = this.start + position;
        return new AnyRecords(file, channel, (int) offset, end, true, offset, path, suffix, file2, currentOffset, this.size);
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
    public int append(MemoryRecords records, long largestOffset) throws IOException {

        System.out.println("!!! AnyRecords append");
        if (records.sizeInBytes() > Integer.MAX_VALUE - (size == null ? 0 : size.get()))
            throw new IllegalArgumentException("Append of size " + records.sizeInBytes() +
                    " bytes is too large for segment with current file position at " + size.get());

        int written = records.writeFullyTo(largestOffset, path, suffix);
        if (size == null)
            size = new AtomicInteger(written);
        else {
            size.addAndGet(written);
        }
        return written;
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
//        if (targetSize > originalSize || targetSize < 0)
//            throw new KafkaException("Attempt to truncate log segment " + file + " to " + targetSize + " bytes failed, " +
//                    " size of this log segment is " + originalSize + " bytes.");
//        if (targetSize < (int) channel.size()) {
//            //channel.truncate(targetSize);
//            size.set(targetSize);
//        }
        return originalSize - targetSize;
    }

    @Override
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        ConvertedRecords<MemoryRecords> convertedRecords = RecordsUtil.downConvert(batches, toMagic, firstOffset, time);
        if (convertedRecords.recordConversionStats().numRecordsConverted() == 0) {
            // This indicates that the message is too large, which means that the buffer is not large
            // enough to hold a full record batch. We just return all the bytes in this instance.
            // Even though the record batch does not have the right format version, we expect old clients
            // to raise an error to the user after reading the record batch size and seeing that there
            // are not enough available bytes in the response to read it fully. Note that this is
            // only possible prior to KIP-74, after which the broker was changed to always return at least
            // one full record batch, even if it requires exceeding the max fetch size requested by the client.
            return new ConvertedRecords<>(this, RecordValidationStats.EMPTY);
        } else {
            return convertedRecords;
        }
    }

    @Override
    public int writeTo(TransferableChannel destChannel, int offset, int length) throws IOException {
        // luke
        System.out.println("!!! write to:" + currentOffset);
//        final StackTraceElement[] elements = Thread.currentThread().getStackTrace();
//        for (int i = 1; i < elements.length; i++) {
//            final StackTraceElement s = elements[i];
//            System.out.println("\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
//        }
        // luke
        List<Long> baseOffsets = listBucket().stream().filter(name -> name.contains(path.substring(1)))
                .map(off -> Long.parseLong(off.substring(off.lastIndexOf('/') + 1, off.lastIndexOf('.')))).sorted().collect(Collectors.toList());
        System.out.println("!!! baseOffsets:" + baseOffsets);
        for (long offset2 : baseOffsets) {
//            long offset2 = Long.parseLong(baseOffset.substring(baseOffset.lastIndexOf('/') + 1, baseOffset.lastIndexOf('.')));
            if (offset2 > currentOffset) {
                currentOffset = offset2;
                break;
            }
        }

        System.out.println("!!! new currentOffset:" + currentOffset);
        if (!file2.containsKey(currentOffset))
            readS3(currentOffset);

//        System.out.println("!!! file2:" + file2 + ";;" + file2.get((long) offset) + ";;" + offset);
        channel = FileChannel.open(file2.get(currentOffset).toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        System.out.println("!!! channel.size():" + channel.size() + ";;" + start + ";;" + end);
        long newSize = Math.min(channel.size(), end) - start;
        int oldSize = sizeInBytes();
//        if (newSize < oldSize)
//            throw new KafkaException(String.format(
//                    "Size of FileRecords %s has been truncated during write: old size %d, new size %d",
//                    path, oldSize, newSize));

        long position = start + offset;
        int count = Math.min(length, oldSize - offset);
        System.out.println("!!! position:" + position + ";;" + count);
        // safe to cast to int since `count` is an int
        return (int) destChannel.transferFrom(channel, 0, count);
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
        for (FileChannelRecordBatch batch : batchesFrom(targetOffset)) {
            long offset = batch.lastOffset();
            System.out.println("!!! offset:" + offset + ";;" + targetOffset);
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
    public Iterable<FileChannelRecordBatch> batchesFrom(final long start) {
        return () -> batchIterator(start);
    }


    public AbstractIterator<FileChannelRecordBatch> batchIterator() {
        return batchIterator(start);
    }

    private AbstractIterator<FileChannelRecordBatch> batchIterator(long start) {
//        final StackTraceElement[] elements = Thread.currentThread().getStackTrace();
//        for (int i = 1; i < elements.length; i++) {
//            final StackTraceElement s = elements[i];
//            System.out.println("\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
//        }
        final int end;
        if (isSlice)
            end = this.sizeInBytes();
        else
            end = this.sizeInBytes();
        System.out.println("!!! isSlice:" + isSlice + ";;" + end + ";;" + sizeInBytes() + ";;" + start);

        FileLogInputStream inputStream = null;
        try {
            if (!file2.containsKey(start)) {
                readS3(start);
            }
            if (file2.isEmpty()) {
                return new RecordBatchIterator<>(null);
            }
//            inputStream = new FileLogInputStream(FileRecords.open(file2.get(start)),0, (int) file2.get(start).length());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new AnyRecordBatchIterator<>(start, path, suffix);
    }

    private void readS3(long start) {
        System.out.println("!!! get S3:" + start);
        if (file2.containsKey(start)) {
            return;
        }
        final StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for (int i = 1; i < elements.length; i++) {
            final StackTraceElement s = elements[i];
            System.out.println("\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
        }
        String accessKey = "minioadmin";
        String secretKey = "minioadmin";
        AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        S3Client s3 = null;
        try {
            s3 = S3Client.builder()
                    .region(Region.US_EAST_1)
                    .endpointOverride(new URI("http://localhost:9000"))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .forcePathStyle(true)
                    .build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket("test")
                .key(path + "/" + start + ".log" + suffix)
                .build();

//        ResponseBytes<GetObjectResponse> s3Object = s3.getObjectAsBytes(objectRequest);
//
//
//        ByteBufferLogInputStream inputStream = new ByteBufferLogInputStream(s3Object.asByteBuffer(), Integer.MAX_VALUE);
//        return new RecordBatchIterator<>(inputStream);
        System.out.println("!!! getting object:" + path + "/" + start + ".log" + suffix);
        Path path = null;
        try {
            path = Files.createTempFile(start + ".log" + suffix, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        GetObjectResponse response = s3.getObject(objectRequest, path);
        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObject(objectRequest, ResponseTransformer.toBytes());
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file.
            file2.put(start, path.toFile());

            OutputStream os = new FileOutputStream(path.toFile());
            os.write(data);
            os.close();
            System.out.println("!!! file:" + path.toFile().length());
//            if (size == null) {
//                size = new AtomicInteger();
//                size.addAndGet((int) path.toFile().length());
//            }
//            size.addAndGet((int) path.toFile().length());
            //        System.out.println("!!! getting object:" + response);

        } catch (Exception e) {
            System.out.println("error while reading s3:" + e);
        }
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
        System.out.println("!!! suffix:" + suffix);
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
    private static FileChannel openChannel(File file,
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

    public static class LogOffsetPosition {
        public final long offset;
        public final int position;
        public final int size;

        public LogOffsetPosition(long offset, int position, int size) {
            this.offset = offset;
            this.position = position;
            this.size = size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            LogOffsetPosition that = (LogOffsetPosition) o;

            return offset == that.offset &&
                    position == that.position &&
                    size == that.size;

        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(offset);
            result = 31 * result + position;
            result = 31 * result + size;
            return result;
        }

        @Override
        public String toString() {
            return "LogOffsetPosition(" +
                    "offset=" + offset +
                    ", position=" + position +
                    ", size=" + size +
                    ')';
        }
    }

    public static class TimestampAndOffset {
        public final long timestamp;
        public final long offset;
        public final Optional<Integer> leaderEpoch;

        public TimestampAndOffset(long timestamp, long offset, Optional<Integer> leaderEpoch) {
            this.timestamp = timestamp;
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimestampAndOffset that = (TimestampAndOffset) o;
            return timestamp == that.timestamp &&
                    offset == that.offset &&
                    Objects.equals(leaderEpoch, that.leaderEpoch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, offset, leaderEpoch);
        }

        @Override
        public String toString() {
            return "TimestampAndOffset(" +
                    "timestamp=" + timestamp +
                    ", offset=" + offset +
                    ", leaderEpoch=" + leaderEpoch +
                    ')';
        }
    }
}
