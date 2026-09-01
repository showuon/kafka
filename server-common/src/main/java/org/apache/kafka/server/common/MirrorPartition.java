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

/**
 * Immutable snapshot of a mirror partition.
 *
 * @param state            the current lifecycle state, or null if unknown
 * @param stateEpoch       monotonically increasing epoch incremented on every state transition
 * @param lastMirrorEpoch  the last mirror leader epoch, or -1 if not yet recorded
 * @param errorMessage     the failure reason when in FAILED state, or null otherwise
 * @param retryAttempt     the retry count in FAILED state, 0 if not failed,
 *                         or {@link #NON_RETRYABLE_ATTEMPT} if non-retryable
 * @param prevState        the state before entering FAILED, or null if not applicable
 */
public record MirrorPartition(MirrorPartitionState state, int stateEpoch, int lastMirrorEpoch,
                              String errorMessage, int retryAttempt, MirrorPartitionState prevState) {
    public static final MirrorPartition EMPTY = new MirrorPartition(MirrorPartitionState.UNKNOWN, 0, -1, null, 0, null);
    public static final int NON_RETRYABLE_ATTEMPT = -1;

    /**
     * Represents the lifecycle states of a mirror partition.
     * Values changes require an update to the JavaDoc of LeaderStateDescription.state().
     */
    public enum MirrorPartitionState {
        LOG_ALIGNMENT((byte) 0),
        EPOCH_FENCING((byte) 1),
        ULE_RECOVERY((byte) 2),
        MIRRORING((byte) 3),
        PAUSING((byte) 4),
        PAUSED((byte) 5),
        STOPPING((byte) 6),
        STOPPED((byte) 7),
        FAILED((byte) 8),
        UNKNOWN((byte) -1);

        private final byte value;

        MirrorPartitionState(byte value) {
            this.value = value;
        }

        public byte value() {
            return value;
        }

        public static MirrorPartitionState fromValue(byte value) {
            switch (value) {
                case 0: return LOG_ALIGNMENT;
                case 1: return EPOCH_FENCING;
                case 2: return ULE_RECOVERY;
                case 3: return MIRRORING;
                case 4: return PAUSING;
                case 5: return PAUSED;
                case 6: return STOPPING;
                case 7: return STOPPED;
                case 8: return FAILED;
                case -1: return UNKNOWN;
            }
            throw new IllegalArgumentException("Illegal mirror state: " + value);
        }
    }

    public static MirrorPartition orEmpty(MirrorPartition mp) {
        return mp != null ? mp : EMPTY;
    }

    public MirrorPartition withState(MirrorPartitionState newState) {
        return new MirrorPartition(newState, stateEpoch, lastMirrorEpoch, errorMessage, retryAttempt, prevState);
    }

    public MirrorPartition withStateEpoch(int newStateEpoch) {
        return new MirrorPartition(state, newStateEpoch, lastMirrorEpoch, errorMessage, retryAttempt, prevState);
    }

    public MirrorPartition withLastMirrorEpoch(int newEpoch) {
        return new MirrorPartition(state, stateEpoch, newEpoch, errorMessage, retryAttempt, prevState);
    }

    public MirrorPartition withError(String errorMessage, int retryAttempt, MirrorPartitionState previousState) {
        return new MirrorPartition(state, stateEpoch, lastMirrorEpoch, errorMessage, retryAttempt, previousState);
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

    public MirrorPartition clearError() {
        return new MirrorPartition(state, stateEpoch, lastMirrorEpoch, null, 0, null);
    }

    public int nextAttempt(boolean isPermFailure) {
        if (isPermFailure || retryAttempt == NON_RETRYABLE_ATTEMPT) {
            return NON_RETRYABLE_ATTEMPT;
        }
        return retryAttempt != 0 ? retryAttempt + 1 : 1;
    }

    public MirrorPartitionState resolvePrevState(MirrorPartitionState currState) {
        if (currState == MirrorPartitionState.FAILED && prevState != null) {
            return prevState;
        }
        return currState;
    }
}
