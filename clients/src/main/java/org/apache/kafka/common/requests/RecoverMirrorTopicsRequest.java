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

import org.apache.kafka.common.message.RecoverMirrorTopicsRequestData;
import org.apache.kafka.common.message.RecoverMirrorTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Set;

public class RecoverMirrorTopicsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<RecoverMirrorTopicsRequest> {

        private final RecoverMirrorTopicsRequestData data;

        public Builder(RecoverMirrorTopicsRequestData data) {
            super(ApiKeys.RECOVER_MIRROR_TOPICS);
            this.data = data;
        }

        public Builder(String mirrorName, Set<String> topics) {
            super(ApiKeys.RECOVER_MIRROR_TOPICS, ApiKeys.RECOVER_MIRROR_TOPICS.oldestVersion(),
                    ApiKeys.RECOVER_MIRROR_TOPICS.latestVersion());
            RecoverMirrorTopicsRequestData data = new RecoverMirrorTopicsRequestData();
            data.setMirrorName(mirrorName);
            topics.forEach(topic -> data.topics().add(new RecoverMirrorTopicsRequestData.TopicMetadata().setTopicName(topic)));
            this.data = data;
        }

        @Override
        public RecoverMirrorTopicsRequest build(short version) {
            return new RecoverMirrorTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final RecoverMirrorTopicsRequestData data;

    public RecoverMirrorTopicsRequest(RecoverMirrorTopicsRequestData data, short version) {
        super(ApiKeys.RECOVER_MIRROR_TOPICS, version);
        this.data = data;
    }

    @Override
    public RecoverMirrorTopicsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        RecoverMirrorTopicsResponseData responseData = new RecoverMirrorTopicsResponseData();
        responseData.setErrorCode(error.code());
        return new RecoverMirrorTopicsResponse(responseData);
    }

    public static RecoverMirrorTopicsRequest parse(Readable readable, short version) {
        return new RecoverMirrorTopicsRequest(
                new RecoverMirrorTopicsRequestData(readable, version),
                version
        );
    }
}
