/*
 * Copyright 2017-present Open Networking Foundation
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
package org.onosproject.store.primitives2;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.onosproject.store.primitives.TransactionId;
import org.onosproject.store.service.CommitStatus;
import org.onosproject.store.service.Serializer;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Default transaction context.
 */
public class DefaultTransactionContext implements TransactionContext {
    private final TransactionCoordinator transactionCoordinator;
    private volatile Transaction transaction;

    public DefaultTransactionContext(TransactionCoordinator transactionCoordinator) {
        this.transactionCoordinator = transactionCoordinator;
    }

    @Override
    public void begin() {
        transaction = new Transaction(TransactionId.from(UUID.randomUUID().toString()));
    }

    @Override
    public CompletableFuture<CommitStatus> commit() {
        return transaction.commit();
    }

    @Override
    public CompletableFuture<Void> rollback() {
        return transaction.rollback();
    }

    @Override
    public <K, V> TransactionalMap<K, V> getMap(String name, Serializer serializer) {
        return transaction.getMap(name, serializer);
    }

    @Override
    public <T> TransactionalSet<T> getSet(String name, Serializer serializer) {
        return transaction.getSet(name, serializer);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("transaction", transaction)
                .toString();
    }
}
