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
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.AbstractIterator;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class AnyRecordBatchIterator<T extends RecordBatch> extends RecordBatchIterator<T> {

    private LogInputStream<T> logInputStream;
    private String path;
    private String suffix;
    private long curOffset = -1;
    private long startOffset = -1;

    AnyRecordBatchIterator(long startOffset, String path, String suffix) {
        this.path = path.substring(1);
        this.suffix = suffix;
        this.startOffset = startOffset;
    }


    private List<String> listBucket() {
        System.out.println("!!! list S3:");
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

    private FileLogInputStream readS3(long start) {
        System.out.println("!!! batchFrom get S3:" + start);
//        final StackTraceElement[] elements = Thread.currentThread().getStackTrace();
//        for (int i = 1; i < elements.length; i++) {
//            final StackTraceElement s = elements[i];
//            System.out.println("\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
//        }
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
//            file2.put(start, path.toFile());

            OutputStream os = new FileOutputStream(path.toFile());
            os.write(data);
            os.close();
//            System.out.println("!!! file:" + path.toFile().length() +  ";;" + file2);

            return new FileLogInputStream(FileRecords.open(path.toFile()),0, (int) path.toFile().length());
        } catch (Exception e) {
            System.out.println("error while reading s3:" + e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T makeNext() {
        try {

            final StackTraceElement[] elements = Thread.currentThread().getStackTrace();
            for (int i = 1; i < elements.length; i++) {
                final StackTraceElement s = elements[i];
                System.out.println("\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
            }
            List<String> baseOffsets = listBucket().stream().filter(name -> name.contains(path)).sorted().collect(Collectors.toList());
            System.out.println("!!! baseOffsets:" + baseOffsets + path + suffix + ";;" + curOffset + ";;" + startOffset);
            for (String baseOffset : baseOffsets) {

                long offset = Long.parseLong(baseOffset.substring(baseOffset.lastIndexOf('/') + 1, baseOffset.lastIndexOf('.')));
                if (offset <= curOffset) {
                    continue;
                }
                curOffset = offset;
                System.out.println("!!! curOffset:" + curOffset);

                T batch = (T) readS3(startOffset).nextBatch();
                System.out.println("!!! batch:" + batch);
                if (batch == null)
                    return allDone();
                return batch;
            }
//            if (logInputStream == null) {
            return allDone();
//            }
//            T batch = logInputStream.nextBatch();
//            if (batch == null)
//                return allDone();
//            return batch;
        } catch (EOFException e) {
            throw new CorruptRecordException("Unexpected EOF while attempting to read the next batch", e);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }
}
