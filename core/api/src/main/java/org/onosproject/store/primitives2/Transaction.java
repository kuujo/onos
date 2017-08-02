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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Sets;
import org.onosproject.store.primitives.TransactionId;
import org.onosproject.store.service.CommitStatus;
import org.onosproject.store.service.Serializer;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Transaction.
 */
public class Transaction {
    private final TransactionId transactionId;
    private final Set<TransactionalPrimitive> participants = Sets.newConcurrentHashSet();

    public Transaction(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public TransactionId transactionId() {
        return transactionId;
    }

    public Collection<TransactionalPrimitive> participants() {
        return participants;
    }

    public <K, V> TransactionalMap<K, V> getMap(String name, Serializer serializer) {

    }

    public <T> TransactionalSet<T> getSet(String name, Serializer serializer) {

    }

    public CompletableFuture<CommitStatus> commit() {

    }

    public CompletableFuture<Void> rollback() {

    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("transactionId", transactionId)
                .toString();
    }
}
