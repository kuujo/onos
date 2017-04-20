/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.store.service;

import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import org.onosproject.store.Timestamp;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Logical timestamp for lock versions.
 * <p>
 * The lock version is a logical timestamp that represents a point in logical time at which a lock was acquired.
 * This can be used in both pessimistic and optimistic locking protocols to ensure that the state of a shared resource
 * has not changed at the end of a transaction.
 */
public class LockVersion implements Timestamp {
    private final long version;

    public LockVersion(long version) {
        this.version = version;
    }

    @Override
    public int compareTo(Timestamp o) {
        checkArgument(o instanceof LockVersion,
                "Must be LockVersion", o);
        LockVersion that = (LockVersion) o;

        return ComparisonChain.start()
                .compare(this.version, that.version)
                .result();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(version);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LockVersion)) {
            return false;
        }
        LockVersion that = (LockVersion) obj;
        return Objects.equals(this.version, that.version);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("version", version)
                .toString();
    }

    /**
     * Returns the lock version.
     *
     * @return the lock version
     */
    public long version() {
        return this.version;
    }
}
