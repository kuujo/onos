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

import org.onosproject.store.primitives.TransactionId;

/**
 * Transaction admin service.
 */
public interface TransactionAdminService extends TransactionService {

    /**
     * Creates a new transaction.
     *
     * @return the transaction ID
     */
    TransactionId createTransaction();

    /**
     * Updates a transaction state.
     *
     * @param transactionId the transaction ID
     * @param transactionState the transaction state
     * @return future to be completed once the transaction state is updated
     */
    CompletableFuture<Void> updateTransaction(TransactionId transactionId, Transaction.State transactionState);

    /**
     * Removes the given transaction from the transaction registry.
     *
     * @param transactionId the transaction identifier
     * @return a completable future to be completed once the transaction state has been removed from the registry
     */
    CompletableFuture<Void> removeTransaction(TransactionId transactionId);

}
