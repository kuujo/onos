/*
 * Copyright 2015-present Open Networking Laboratory
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

import org.onosproject.store.primitives.DistributedPrimitiveBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract base class for a transaction context builder.
 */
public abstract class TransactionContextBuilder
    extends DistributedPrimitiveBuilder<TransactionContextBuilder, TransactionContext> {

    protected LockMode lockMode = LockMode.OPTIMISTIC;
    protected IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READ;

    public TransactionContextBuilder() {
        super(DistributedPrimitive.Type.TRANSACTION_CONTEXT);
    }

    /**
     * Sets the transaction lock mode.
     *
     * @param lockMode the transaction lock mode
     * @return the transaction builder
     */
    public TransactionContextBuilder withLockMode(LockMode lockMode) {
        this.lockMode = checkNotNull(lockMode);
        return this;
    }

    /**
     * Sets the transaction isolation level.
     *
     * @param isolationLevel the isolation level
     * @return the transaction builder
     */
    public TransactionContextBuilder withIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = checkNotNull(isolationLevel);
        return this;
    }
}
