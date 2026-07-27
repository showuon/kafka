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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.server.common.MirrorPartitionState;

/**
 * Immutable snapshot of a mirror partition.
 *
 * @param state            the current lifecycle state, or null if unknown
 * @param lastMirrorEpoch  the last mirrored leader epoch, or -1 if not yet recorded
 * @param errorMessage     the failure reason when in FAILED state, or null otherwise
 * @param retryAttempt     the retry count in FAILED state, 0 if not failed,
 *                         or {@link #NON_RETRYABLE_ATTEMPT} if non-retryable
 * @param prevState        the state before entering FAILED, or null if not applicable
 */
public record MirrorPartition(MirrorPartitionState state, int lastMirrorEpoch,
                              String errorMessage, int retryAttempt, MirrorPartitionState prevState) {
    public static final MirrorPartition EMPTY = new MirrorPartition(null, -1, null, 0, null);
    public static final int NON_RETRYABLE_ATTEMPT = -1;

    public static MirrorPartition orEmpty(MirrorPartition mp) {
        return mp != null ? mp : EMPTY;
    }

    public MirrorPartition withState(MirrorPartitionState newState) {
        return new MirrorPartition(newState, lastMirrorEpoch, errorMessage, retryAttempt, prevState);
    }

    public MirrorPartition withLastMirrorEpoch(int newEpoch) {
        return new MirrorPartition(state, newEpoch, errorMessage, retryAttempt, prevState);
    }

    public MirrorPartition withError(String errorMessage, int retryAttempt, MirrorPartitionState previousState) {
        return new MirrorPartition(state, lastMirrorEpoch, errorMessage, retryAttempt, previousState);
    }

    public int nextAttempt(boolean nonRetryable) {
        if (nonRetryable || retryAttempt == NON_RETRYABLE_ATTEMPT) {
            return NON_RETRYABLE_ATTEMPT;
        }
        return retryAttempt != 0 ? retryAttempt + 1 : 1;
    }

    public MirrorPartitionState resolvePreviousState(MirrorPartitionState currentState) {
        if (currentState == MirrorPartitionState.FAILED && prevState != null) {
            return prevState;
        }
        return currentState;
    }
}
