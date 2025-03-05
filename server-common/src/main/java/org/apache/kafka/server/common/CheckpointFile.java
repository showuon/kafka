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
package org.apache.kafka.server.common;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.storage.StorageManager;
import org.apache.kafka.common.utils.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a utility to capture a checkpoint in a file. It writes down to the file in the below format.
 * <pre>
 * ========= File beginning =========
 * version: int
 * entries-count: int
 * entry-as-string-on-each-line
 * ========= File end ===============
 * </pre>
 * Each entry is represented as a string on each line in the checkpoint file. {@link EntryFormatter} is used
 * to convert the entry into a string and vice versa.
 *
 * @param <T> entry type.
 */
public class CheckpointFile<T> {

    private final int version;
    private final EntryFormatter<T> formatter;
    private final Object lock = new Object();
    private final Path absolutePath;
    private final StorageManager storageManager;

    public CheckpointFile(File file,
                          int version,
                          EntryFormatter<T> formatter,
                          StorageManager storageManager) throws IOException {
        this.version = version;
        this.formatter = formatter;
        this.storageManager = storageManager;
        absolutePath = file.toPath().toAbsolutePath();
    }

    public void write(Collection<T> entries) throws IOException {
        synchronized (lock) {
            CheckpointWriteBuffer<T> checkpointWriteBuffer = new CheckpointWriteBuffer<>(version, formatter);
            ByteBuffer buffer = ByteBuffer.wrap(checkpointWriteBuffer.write(entries).getBytes());
            storageManager.append(absolutePath.toString(), buffer, StorageManager.StorageType.CHECKPOINT);
        }
    }

    public List<T> read() throws IOException {
        synchronized (lock) {
            ByteBuffer buffer = ByteBuffer.allocate((int) storageManager.size(absolutePath.toString(), StorageManager.StorageType.CHECKPOINT));
            storageManager.read(absolutePath.toString(), buffer, 0, StorageManager.StorageType.CHECKPOINT);
            buffer.flip();

            CheckpointReadBuffer<T> checkpointBuffer = new CheckpointReadBuffer<>(absolutePath.toString(), version, formatter);
            return checkpointBuffer.read(buffer);
        }
    }

    public static class CheckpointWriteBuffer<T> {
        private final int version;
        private final EntryFormatter<T> formatter;

        public CheckpointWriteBuffer(int version,
                                     EntryFormatter<T> formatter) {
            this.version = version;
            this.formatter = formatter;
        }

        public String write(Collection<T> entries) throws IOException {
            StringBuilder stringBuilder = new StringBuilder();
            // Write the version
            stringBuilder.append(version);
            stringBuilder.append(System.lineSeparator());

            // Write the entries count
            stringBuilder.append(entries.size());
            stringBuilder.append(System.lineSeparator());

            // Write each entry on a new line.
            for (T entry : entries) {
                stringBuilder.append(formatter.toString(entry));
                stringBuilder.append(System.lineSeparator());
            }
            return stringBuilder.toString();
        }
    }

    public static class CheckpointReadBuffer<T> {

        private final String location;
        private final int version;
        private final EntryFormatter<T> formatter;

        public CheckpointReadBuffer(String location,
                             int version,
                             EntryFormatter<T> formatter) {
            this.location = location;
            this.version = version;
            this.formatter = formatter;
        }

        public List<T> read(ByteBuffer buffer) throws IOException {
            String content = StandardCharsets.UTF_8.decode(buffer).toString();
            List<String> lines = content.lines().toList();
            if (lines.isEmpty()) {
                return Collections.emptyList();
            }
            String line = lines.get(0);
            int readVersion = toInt(line);
            if (readVersion != version) {
                throw new IOException("Unrecognised version:" + readVersion + ", expected version: " + version
                                              + " in checkpoint file at: " + location);
            }

            if (lines.size() == 1) {
                return Collections.emptyList();
            }
            line = lines.get(1);
            int expectedSize = toInt(line);
            List<T> entries = new ArrayList<>(expectedSize);

            for (int i = 2; i < lines.size(); i++) {
                line = lines.get(i);
                Optional<T> maybeEntry = formatter.fromString(line);
                if (maybeEntry.isEmpty()) {
                    throw buildMalformedLineException(line);
                }
                entries.add(maybeEntry.get());
            }

            if (entries.size() != expectedSize) {
                throw new IOException("Expected [" + expectedSize + "] entries in checkpoint file ["
                                              + location + "], but found only [" + entries.size() + "]");
            }

            return entries;
        }

        private int toInt(String line) throws IOException {
            try {
                return Integer.parseInt(line);
            } catch (NumberFormatException e) {
                throw buildMalformedLineException(line);
            }
        }

        private IOException buildMalformedLineException(String line) {
            return new IOException(String.format("Malformed line in checkpoint file [%s]: %s", location, line));
        }
    }

    /**
     * This is used to convert the given entry of type {@code T} into a string and vice versa.
     *
     * @param <T> entry type
     */
    public interface EntryFormatter<T> {

        /**
         * @param entry entry to be converted into string.
         * @return String representation of the given entry.
         */
        String toString(T entry);

        /**
         * @param value string representation of an entry.
         * @return entry converted from the given string representation if possible. {@link Optional#empty()} represents
         * that the given string representation could not be converted into an entry.
         */
        Optional<T> fromString(String value);
    }
}
