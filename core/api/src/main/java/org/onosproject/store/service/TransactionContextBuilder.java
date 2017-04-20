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

    protected IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READ;

    protected TransactionContextBuilder() {
        super(DistributedPrimitive.Type.TRANSACTION_CONTEXT);
    }

    /**
     * Sets the transaction isolation level.
     * <p>
     * The isolation level dictates how the transaction is coordinated with concurrent transactions. Stronger isolation
     * levels like {@link IsolationLevel#SERIALIZABLE} will ensure that no other transaction can modified shared state
     * during the course of this transaction, while weaker isolation levels will sacrifice those consistency guarantees
     * in exchange for better performance.
     *
     * @param isolationLevel the transaction isolation level
     * @return the transaction context builder
     * @see IsolationLevel
     */
    public TransactionContextBuilder withIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = checkNotNull(isolationLevel);
        return this;
    }

    /**
     * Configures the transaction context with the {@link IsolationLevel#SERIALIZABLE SERIALIZABLE} isolation level.
     *
     * @return the transaction context builder
     */
    public TransactionContextBuilder serializable() {
        return withIsolationLevel(IsolationLevel.SERIALIZABLE);
    }

    /**
     * Configures the transaction context the with {@link IsolationLevel#SNAPSHOT SNAPSHOT} isolation level.
     *
     * @return the transaction context builder
     */
    public TransactionContextBuilder snapshot() {
        return withIsolationLevel(IsolationLevel.SNAPSHOT);
    }

    /**
     * Configures the transaction context with the {@link IsolationLevel#REPEATABLE_READ REPEATABLE_READ} isolation
     * level.
     *
     * @return the transaction context builder
     */
    public TransactionContextBuilder repeatableRead() {
        return withIsolationLevel(IsolationLevel.REPEATABLE_READ);
    }

    /**
     * Configures the transaction context with the {@link IsolationLevel#READ_COMMITTED READ_COMMITTED} isolation level.
     *
     * @return the transaction context builder
     */
    public TransactionContextBuilder readCommitted() {
        return withIsolationLevel(IsolationLevel.READ_COMMITTED);
    }

    /**
     * Configures the transaction context with the {@link IsolationLevel#READ_UNCOMMITTED READ_UNCOMMITTED} isolation
     * level.
     *
     * @return the transaction context builder
     */
    public TransactionContextBuilder readUncommitted() {
        return withIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    }

}
