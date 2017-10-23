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
package org.onosproject.store.service;

import java.util.Collection;

import org.onosproject.store.primitives.TransactionId;

/**
 * Transaction service.
 */
public interface TransactionService {

    /**
     * Returns all pending transactions.
     *
     * @return collection of pending transaction identifiers.
     */
    Collection<TransactionId> getPendingTransactions();

    /**
     * Returns a partitioned transactional map for use within a transaction context.
     * <p>
     * The transaction coordinator will return a map that takes advantage of caching that's shared across transaction
     * contexts.
     *
     * @param name the map name
     * @param serializer the map serializer
     * @param transactionId the transaction ID for which the map is being created
     * @param <K> key type
     * @param <V> value type
     * @return a partitioned transactional map
     */
    <K, V> TransactionalMap<K, V> getTransactionalMap(
            String name,
            Serializer serializer,
            TransactionId transactionId);

}
