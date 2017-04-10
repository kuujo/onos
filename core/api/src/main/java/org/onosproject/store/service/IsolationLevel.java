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

/**
 * Transaction isolation level.
 * <p>
 * The isolation level dictates the visibility of concurrent transactions during the lifetime of a transaction.
 * Strong isolation levels reduce the affect of concurrent transactions but are often more costly in terms of
 * performance, while weaker isolation levels sacrifice safety for performance. Each supported isolation level
 * is documented with its specific guarantees.
 */
public enum IsolationLevel {

    /**
     * Serializable isolation level.
     * <p>
     * The {@code SERIALIZABLE} isolation level guarantees that transactions will run in a serialized manner in which
     * no two transactions will overlap in logical time. Serializability may be guaranteed by using optimistic or
     * pessimistic concurrency control. This is useful in particular when changes to a primitive or key are made based
     * on the value of another primitive or key. Lower isolation levels cannot guarantee the atomicity of such
     * check-and-set operations that span across resources since locking is only done on keys that are modified within
     * a transaction.
     */
    SERIALIZABLE,

    /**
     * Snapshot isolation level.
     * <p>
     * The {@code SNAPSHOT} isolation level guarantees that within the transaction, reads occur at a single point in
     * time on a logical snapshot of the data. Two transactions running concurrently may see different snapshots, and
     * only write conflicts are prevented.
     */
    SNAPSHOT,

    /**
     * Repeatable read isolation level.
     * <p>
     * {@code REPEATABLE_READ} ensures that once a value is read within the transaction, the value will not be changed
     * by any other transaction. For example, this isolation level ensures the following assertion will always succeed,
     * regardless of whether another concurrent transaction alters the {@code foo} map:
     * <pre>
     *     {@code
     *     TransactionalMap<String, String> map = transactionContext.getTransactionalMap("foo", Serializers.API);
     *     assert map.get("foo").equals(map.get("foo"));
     *     }
     * </pre>
     * Note, however, that this does not preclude a concurrent transaction from altering the map during the transaction.
     * {@code REPEATABLE_READ} merely ensures the map will not be altered from the perspective of the current
     * transaction during its lifetime.
     */
    REPEATABLE_READ,

    /**
     * Read committed isolation level.
     * <p>
     * {@code READ_COMMITTED} allows the transaction to read the most recent committed data from a transaction. This can
     * mean that the value of a key in a map may change during the course of a transaction, so the example given in the
     * {@link #REPEATABLE_READ} documentation does not hold.
     */
    READ_COMMITTED,

    /**
     * Read uncommitted isolation level.
     * <p>
     * The {@code READ_UNCOMMITTED} isolation level allows for changes made by in-progress transactions to be seen.
     * However, currently such dirty reads are not actually allowed by existing primitives, so this isolation level
     * actually produces {@link #READ_COMMITTED} isolation.
     */
    READ_UNCOMMITTED,

}
