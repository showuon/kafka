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
 * Immutable snapshot of a mirror partition.
 *
 * @param state                 the current lifecycle state, or null if unknown
 * @param stateEpoch            monotonically increasing epoch incremented on every state transition
 * @param lastMirrorPosition    the last mirrored epoch and offset, or {@link EpochOffset#EMPTY} if not yet recorded
 * @param errorMessage          the failure reason when in FAILED state, or null otherwise
 * @param retryAttempt          the retry count in FAILED state, 0 if not failed,
 *                              or {@link #NON_RETRYABLE_ATTEMPT} if non-retryable
 * @param prevState             the state before entering FAILED, or null if not applicable
 */
public record MirrorPartitionMetadata(MirrorPartitionState state, int stateEpoch, EpochOffset lastMirrorPosition,
                                      String errorMessage, int retryAttempt, MirrorPartitionState prevState) {
    public static final MirrorPartitionMetadata EMPTY = new MirrorPartitionMetadata(MirrorPartitionState.UNKNOWN, 0, EpochOffset.EMPTY, null, 0, null);
    public static final int NON_RETRYABLE_ATTEMPT = -1;

    public static MirrorPartitionMetadata orEmpty(MirrorPartitionMetadata mpm) {
        return mpm != null ? mpm : EMPTY;
    }

    public int lastMirrorEpoch() {
        return lastMirrorPosition.epoch();
    }

    public long lastMirrorOffset() {
        return lastMirrorPosition.offset();
    }

    public MirrorPartitionMetadata withState(MirrorPartitionState newState) {
        return new MirrorPartitionMetadata(newState, stateEpoch, lastMirrorPosition, errorMessage, retryAttempt, prevState);
    }

    public MirrorPartitionMetadata withStateEpoch(int newStateEpoch) {
        return new MirrorPartitionMetadata(state, newStateEpoch, lastMirrorPosition, errorMessage, retryAttempt, prevState);
    }

    public MirrorPartitionMetadata withLastMirrorPosition(EpochOffset newLastMirrorPosition) {
        return new MirrorPartitionMetadata(state, stateEpoch, newLastMirrorPosition, errorMessage, retryAttempt, prevState);
    }

    public MirrorPartitionMetadata withLastMirrorEpoch(int newEpoch) {
        return withLastMirrorPosition(new EpochOffset(newEpoch, lastMirrorPosition.offset()));
    }

    public MirrorPartitionMetadata withLastMirrorOffset(long newOffset) {
        return withLastMirrorPosition(new EpochOffset(lastMirrorPosition.epoch(), newOffset));
    }

    public MirrorPartitionMetadata withError(String errorMessage, int retryAttempt, MirrorPartitionState previousState) {
        return new MirrorPartitionMetadata(state, stateEpoch, lastMirrorPosition, errorMessage, retryAttempt, previousState);
    }

    @SuppressWarnings({"cyclomaticComplexity", "BooleanExpressionComplexity"})
    public static boolean isValidStateTransition(MirrorPartitionState source, MirrorPartitionState target) {
        if (source == target) {
            return true;
        }
        switch (target) {
            case LOG_ALIGNMENT:
                return source == null
                        || source == MirrorPartitionState.UNKNOWN
                        || source == MirrorPartitionState.STOPPED
                        || source == MirrorPartitionState.FAILED;
            case EPOCH_FENCING:
                return source == MirrorPartitionState.MIRRORING;
            case ULE_RECOVERY:
                return source == MirrorPartitionState.MIRRORING;
            case MIRRORING:
                return source == MirrorPartitionState.LOG_ALIGNMENT
                        || source == MirrorPartitionState.EPOCH_FENCING
                        || source == MirrorPartitionState.PAUSED
                        || source == MirrorPartitionState.ULE_RECOVERY
                        || source == MirrorPartitionState.FAILED
                        || source == MirrorPartitionState.MIRRORING;
            case PAUSING:
                return source == MirrorPartitionState.MIRRORING;
            case PAUSED:
                return source == MirrorPartitionState.PAUSING;
            case STOPPING:
                return source == MirrorPartitionState.LOG_ALIGNMENT
                        || source == MirrorPartitionState.EPOCH_FENCING
                        || source == MirrorPartitionState.MIRRORING
                        || source == MirrorPartitionState.PAUSING
                        || source == MirrorPartitionState.PAUSED
                        || source == MirrorPartitionState.ULE_RECOVERY
                        || source == MirrorPartitionState.FAILED;
            case STOPPED:
                return source == MirrorPartitionState.STOPPING;
            case FAILED:
                return true;
            default:
                return false;
        }
    }

    public MirrorPartitionMetadata clearError() {
        return new MirrorPartitionMetadata(state, stateEpoch, lastMirrorPosition, null, 0, null);
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
