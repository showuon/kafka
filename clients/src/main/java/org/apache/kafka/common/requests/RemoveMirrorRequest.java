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

import org.apache.kafka.common.message.RemoveMirrorRequestData;
import org.apache.kafka.common.message.RemoveMirrorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.List;

public class RemoveMirrorRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<RemoveMirrorRequest> {

        private final RemoveMirrorRequestData data;

        public Builder(RemoveMirrorRequestData data) {
            super(ApiKeys.REMOVE_MIRROR);
            this.data = data;
        }

        public Builder(List<String> mirrorNames) {
            super(ApiKeys.REMOVE_MIRROR, ApiKeys.REMOVE_MIRROR.oldestVersion(),
                  ApiKeys.REMOVE_MIRROR.latestVersion());
            RemoveMirrorRequestData data = new RemoveMirrorRequestData();
            mirrorNames.forEach(name -> data.mirrorName().add(name));
            this.data = data;
        }

        @Override
        public RemoveMirrorRequest build(short version) {
            return new RemoveMirrorRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final RemoveMirrorRequestData data;

    public RemoveMirrorRequest(RemoveMirrorRequestData data, short version) {
        super(ApiKeys.REMOVE_MIRROR, version);
        this.data = data;
    }

    @Override
    public RemoveMirrorRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        RemoveMirrorResponseData responseData = new RemoveMirrorResponseData();
        responseData.setThrottleTimeMs(throttleTimeMs);
        responseData.setErrorCode(error.code());
        data.mirrorName().forEach(name -> responseData.mirrorResponse().add(
            new RemoveMirrorResponseData.MirrorResponse()
                .setName(name)
                .setErrorCode(error.code())));
        return new RemoveMirrorResponse(responseData);
    }

    public static RemoveMirrorRequest parse(Readable readable, short version) {
        return new RemoveMirrorRequest(
                new RemoveMirrorRequestData(readable, version),
                version
        );
    }
}
