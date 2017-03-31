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

import java.util.concurrent.CompletableFuture;

/**
 * Transactional distributed primitive interface.
 */
public interface Transactional extends DistributedPrimitive {

    /**
     * Begins the given transaction.
     *
     * @param scope the current transaction scope
     * @return a completable future to be completed once the primitive is prepared
     */
    CompletableFuture<Void> begin(TransactionScope scope);

    /**
     * Prepares the transaction for commitment.
     *
     * @return a completable future to be completed once the primitive is prepared
     */
    CompletableFuture<Boolean> prepare();

    /**
     * Commits the current transaction.
     *
     * @return a completable future to be completed once the transaction has been committed
     */
    CompletableFuture<Boolean> commit();

    /**
     * Rolls back the given transaction.
     *
     * @return a completable future to be completed once the transaction has been rolled back
     */
    CompletableFuture<Boolean> rollback();

}
