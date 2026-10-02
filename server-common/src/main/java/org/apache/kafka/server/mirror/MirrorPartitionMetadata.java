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
package org.apache.kafka.server.mirror;

import org.apache.kafka.common.EpochOffset;

/**
 * Immutable snapshot of mirror partition metadata.
 *
 * @param state          the current lifecycle state, or null if unknown
 * @param stateEpoch     monotonically increasing epoch incremented on every state transition
 * @param lastPosition   the last mirrored epoch and offset, or {@link EpochOffset#EMPTY} if not yet recorded
 * @param errorMessage   the failure reason when in FAILED state, or null otherwise
 * @param retryAttempt   the retry count in FAILED state, 0 if not failed,
 *                       or {@link #NON_RETRYABLE_ATTEMPT} if non-retryable
 * @param prevState      the state before entering FAILED, or null if not applicable
 */
public record MirrorPartitionMetadata(MirrorPartitionState state, int stateEpoch, EpochOffset lastPosition,
                                      String errorMessage, int retryAttempt, MirrorPartitionState prevState) {
    public static final MirrorPartitionMetadata EMPTY = new MirrorPartitionMetadata(MirrorPartitionState.UNKNOWN, 0, EpochOffset.EMPTY, null, 0, null);
    public static final int NON_RETRYABLE_ATTEMPT = -1;

    public static MirrorPartitionMetadata orEmpty(MirrorPartitionMetadata mpm) {
        return mpm != null ? mpm : EMPTY;
    }

    public static class Builder {
        private MirrorPartitionState state;
        private int stateEpoch;
        private EpochOffset lastPosition;
        private String errorMessage;
        private int retryAttempt;
        private MirrorPartitionState prevState;

        public Builder() {
            this.state = EMPTY.state();
            this.stateEpoch = EMPTY.stateEpoch();
            this.lastPosition = EMPTY.lastPosition();
            this.errorMessage = EMPTY.errorMessage();
            this.retryAttempt = EMPTY.retryAttempt();
            this.prevState = EMPTY.prevState();
        }

        public Builder(MirrorPartitionMetadata existing) {
            existing = orEmpty(existing);
            this.state = existing.state();
            this.stateEpoch = existing.stateEpoch();
            this.lastPosition = existing.lastPosition();
            this.errorMessage = existing.errorMessage();
            this.retryAttempt = existing.retryAttempt();
            this.prevState = existing.prevState();
        }

        public Builder withState(MirrorPartitionState state) {
            this.state = state;
            return this;
        }

        public Builder withStateEpoch(int stateEpoch) {
            this.stateEpoch = stateEpoch;
            return this;
        }

        public Builder withLastPosition(EpochOffset lastPosition) {
            this.lastPosition = lastPosition;
            return this;
        }

        public Builder withErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder withRetryAttempt(int retryAttempt) {
            this.retryAttempt = retryAttempt;
            return this;
        }

        public Builder withPrevState(MirrorPartitionState prevState) {
            this.prevState = prevState;
            return this;
        }

        /**
         * Sets error-related fields (errorMessage, retryAttempt, prevState) based on the state transition.
         */
        public Builder withErrorInfoOnState(MirrorPartitionState newState, String errorMessage,
                                            boolean nonRetryable, int maxRetryAttempts) {
            if (newState == MirrorPartitionState.FAILED) {
                MirrorPartitionMetadata current = build();
                int attempt = current.nextAttempt(nonRetryable, maxRetryAttempts);
                MirrorPartitionState previousState = current.resolvePrevState(this.state);
                this.errorMessage = errorMessage;
                this.retryAttempt = attempt;
                this.prevState = previousState;
            } else if ((this.state != MirrorPartitionState.FAILED && this.state != newState)
                    || newState == MirrorPartitionState.STOPPED
                    || newState == MirrorPartitionState.PAUSED) {
                this.errorMessage = null;
                this.retryAttempt = 0;
                this.prevState = null;
            } else {
                this.errorMessage = errorMessage;
            }
            return this;
        }

        public MirrorPartitionMetadata build() {
            return new MirrorPartitionMetadata(state, stateEpoch, lastPosition,
                    errorMessage, retryAttempt, prevState);
        }
    }

    @SuppressWarnings({"cyclomaticComplexity", "BooleanExpressionComplexity"})
    public static boolean isValidStateTransition(MirrorPartitionState source, MirrorPartitionState target) {
        if (source == target || source == MirrorPartitionState.FAILED) {
            return true;
        }
        switch (target) {
            case LOG_ALIGNMENT:
                return source == null
                        || source == MirrorPartitionState.UNKNOWN
                        || source == MirrorPartitionState.STOPPED;
            case EPOCH_FENCING, ULE_RECOVERY, PAUSING:
                return source == MirrorPartitionState.MIRRORING;
            case MIRRORING:
                return source == MirrorPartitionState.LOG_ALIGNMENT
                        || source == MirrorPartitionState.EPOCH_FENCING
                        || source == MirrorPartitionState.PAUSED
                        || source == MirrorPartitionState.ULE_RECOVERY;
            case PAUSED:
                return source == MirrorPartitionState.PAUSING;
            case STOPPING:
                return source == MirrorPartitionState.LOG_ALIGNMENT
                        || source == MirrorPartitionState.EPOCH_FENCING
                        || source == MirrorPartitionState.MIRRORING
                        || source == MirrorPartitionState.PAUSING
                        || source == MirrorPartitionState.PAUSED
                        || source == MirrorPartitionState.ULE_RECOVERY;
            case STOPPED:
                return source == MirrorPartitionState.STOPPING;
            case FAILED:
                return true;
            default:
                return false;
        }
    }

    public int nextAttempt(boolean nonRetryable, int maxAttempts) {
        if (nonRetryable || retryAttempt == NON_RETRYABLE_ATTEMPT) {
            return NON_RETRYABLE_ATTEMPT;
        }
        return retryAttempt != 0 ? Math.min(maxAttempts, retryAttempt + 1) : 1;
    }

    public MirrorPartitionState resolvePrevState(MirrorPartitionState currState) {
        if (currState == MirrorPartitionState.FAILED && prevState != null) {
            return prevState;
        }
        return currState;
    }
}
