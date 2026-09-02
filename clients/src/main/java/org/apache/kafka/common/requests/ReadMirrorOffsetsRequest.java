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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ReadMirrorOffsetsRequestData;
import org.apache.kafka.common.message.ReadMirrorOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class ReadMirrorOffsetsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ReadMirrorOffsetsRequest> {

        private final ReadMirrorOffsetsRequestData data;

        public Builder(ReadMirrorOffsetsRequestData data) {
            super(ApiKeys.READ_MIRROR_OFFSETS);
            this.data = data;
        }

        @Override
        public ReadMirrorOffsetsRequest build(short version) {
            return new ReadMirrorOffsetsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ReadMirrorOffsetsRequestData data;

    public ReadMirrorOffsetsRequest(ReadMirrorOffsetsRequestData data, short version) {
        super(ApiKeys.READ_MIRROR_OFFSETS, version);
        this.data = data;
    }

    @Override
    public ReadMirrorOffsetsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        ReadMirrorOffsetsResponseData responseData = new ReadMirrorOffsetsResponseData();
        responseData.setErrorCode(error.code());
        return new ReadMirrorOffsetsResponse(responseData);
    }

    public static ReadMirrorOffsetsRequest parse(Readable readable, short version) {
        return new ReadMirrorOffsetsRequest(
                new ReadMirrorOffsetsRequestData(readable, version),
                version
        );
    }
}
