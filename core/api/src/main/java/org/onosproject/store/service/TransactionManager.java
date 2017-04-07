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
 * Transaction manager.
 */
public interface TransactionManager {

    /**
     * Executes the given transaction block using the given transaction context.
     *
     * @param context the context with which to execute the transaction block
     * @param block the transaction block to execute
     * @param <T> the transaction output type
     * @return a completable future to be completed once the transaction is complete
     */
    <T> CompletableFuture<T> execute(TransactionContext context, TransactionBlock<T> block);

}
