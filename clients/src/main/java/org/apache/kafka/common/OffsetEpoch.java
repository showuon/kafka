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
package org.apache.kafka.common;

import java.util.Objects;

/**
 * Pairs a leader epoch with a log offset, used to record the last mirrored
 * position of a partition before mirroring stops.
 */
public final class OffsetEpoch {
    public static final OffsetEpoch EMPTY = new OffsetEpoch(-1, -1L);

    private final int epoch;
    private final long offset;

    public OffsetEpoch(int epoch, long offset) {
        this.epoch = epoch;
        this.offset = offset;
    }

    /** The last mirror leader epoch, or -1 if not yet recorded. */
    public int epoch() {
        return epoch;
    }

    /** The last mirror offset, or -1 if not yet recorded. */
    public long offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetEpoch that = (OffsetEpoch) o;
        return epoch == that.epoch && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, offset);
    }

    @Override
    public String toString() {
        return "OffsetEpoch(epoch=" + epoch + ", offset=" + offset + ")";
    }
}
