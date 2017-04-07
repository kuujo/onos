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

import org.onosproject.store.primitives.TransactionId;

/**
 * Transaction.
 */
public interface Transaction<T> {

    /**
     * Transaction state.
     */
    enum State {

        /**
         * Active state.
         */
        ACTIVE,

        /**
         * Preparing state.
         */
        PREPARING,

        /**
         * Prepared state.
         */
        PREPARED,

        /**
         * Committing state.
         */
        COMMITTING,

        /**
         * Committed state.
         */
        COMMITTED,

        /**
         * Rolling back state.
         */
        ROLLING_BACK,

        /**
         * Rolled back state.
         */
        ROLLED_BACK,
    }

    /**
     * Returns the transaction ID.
     *
     * @return the transaction ID
     */
    TransactionId transactionId();

    /**
     * Returns the transaction log.
     *
     * @return the transaction log
     */
    TransactionLog<T> log();

}
