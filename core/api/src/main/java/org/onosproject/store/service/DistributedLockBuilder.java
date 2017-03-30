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

import org.onosproject.store.primitives.DistributedLock;
import org.onosproject.store.primitives.DistributedPrimitiveBuilder;

/**
 * Distributed lock builder.
 */
public abstract class DistributedLockBuilder
        extends DistributedPrimitiveBuilder<DistributedLockBuilder, DistributedLock> {

    protected boolean reentrant;

    public DistributedLockBuilder() {
        super(DistributedPrimitive.Type.LOCK);
    }

    /**
     * Sets whether the lock is reentrant.
     * <p>
     * Reentrant locks use a hold counter internally to allow the lock to be repeatedly locked by the same
     * process. Reentrant locks must be unlocked the same number of times as they were locked to release the lock.
     *
     * @param reentrant whether to construct a reentrant lock
     * @return the lock builder
     */
    public DistributedLockBuilder withReentrant(boolean reentrant) {
        this.reentrant = reentrant;
        return this;
    }

    /**
     * Sets the lock as reentrant.
     * <p>
     * Reentrant locks use a hold counter internally to allow the lock to be repeatedly locked by the same
     * process. Reentrant locks must be unlocked the same number of times as they were locked to release the lock.
     *
     * @return the lock builder
     */
    public DistributedLockBuilder reentrant() {
        return withReentrant(true);
    }

    /**
     * Builds an asynchronous distributed lock.
     *
     * @return a new asynchronous distributed lock
     */
    public abstract AsyncDistributedLock buildAsyncLock();

}
