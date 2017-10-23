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

import java.util.concurrent.CompletableFuture;

/**
 * Transaction coordinator.
 */
public interface TransactionCoordinator {

    /**
     * Returns a transactional map for this transaction.
     *
     * @param name the transactional map name
     * @param serializer the serializer
     * @param <K> key type
     * @param <V> value type
     * @return a transactional map for this transaction
     */
    <K, V> TransactionalMap<K, V> getTransactionalMap(String name, Serializer serializer);

    /**
     * Commits the transaction.
     *
     * @return the transaction commit status
     */
    CompletableFuture<CommitStatus> commit();

}