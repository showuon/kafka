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
package org.apache.kafka.common.errors;

/**
 * This source metadata in mirror is not available.
 * This exception is used when cluster mirroring that the metadata is not retrieved yet.
 */
public class SourceMetadataNotAvailableException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public SourceMetadataNotAvailableException() {
    }

    public SourceMetadataNotAvailableException(String message) {
        super(message);
    }

    public SourceMetadataNotAvailableException(Throwable throwable) {
        super(throwable);
    }

    public SourceMetadataNotAvailableException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
