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

package org.onosproject.store.primitives.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.ConsistentMap;
import org.onosproject.store.service.LockVersion;
import org.onosproject.store.service.TransactionException;
import org.onosproject.store.service.TransactionalMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link TransactionalMap} implementation that manages the transaction within a single partition and isolates reads
 * within the transaction based on the provided {@link Transaction}.
 *
 * @param <K> key type
 * @param <V> value type.
 */
public abstract class TransactionalMapParticipant<K, V> implements TransactionalMap<K, V>, TransactionParticipant {
    private static final String TX_CLOSED_ERROR = "Transaction is closed";
    private static final String ERROR_NULL_VALUE = "Null values are not allowed";
    private static final String ERROR_NULL_KEY = "Null key is not allowed";

    protected final AsyncConsistentMap<K, V> backingMap;
    protected final ConsistentMap<K, V> backingConsistentMap;
    protected final Transaction<MapRecord<K, V>> transaction;
    protected final Map<K, V> writeCache = Maps.newConcurrentMap();
    protected final Set<K> deleteSet = Sets.newConcurrentHashSet();
    protected final List<MapRecord<K, V>> log = new ArrayList<>();
    protected volatile LockVersion lock;

    protected TransactionalMapParticipant(
            AsyncConsistentMap<K, V> backingMap,
            Transaction<MapRecord<K, V>> transaction) {
        this.backingMap = backingMap;
        this.backingConsistentMap = backingMap.asConsistentMap();
        this.transaction = transaction;
    }

    /**
     * Starts the transaction for this partition when a read occurs.
     * <p>
     * Acquiring a pessimistic lock at the start of the transaction ensures that underlying cached maps have been
     * synchronized prior to a read.
     */
    private void beginTransaction() {
        if (!transaction.isOpen()) {
            try {
                lock = transaction.begin().get(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TransactionException.Interrupted();
            } catch (TimeoutException e) {
                throw new TransactionException.Timeout();
            } catch (ExecutionException e) {
                Throwables.propagateIfPossible(e.getCause());
                throw new TransactionException(e.getCause());
            }
        }
    }

    @Override
    public V get(K key) {
        // Start the transaction for this primitive/partition if necessary.
        beginTransaction();

        checkState(transaction.isOpen(), TX_CLOSED_ERROR);
        checkNotNull(key, ERROR_NULL_KEY);

        if (deleteSet.contains(key)) {
            return null;
        }

        V latest = writeCache.get(key);
        if (latest != null) {
            return latest;
        } else {
            return read(key);
        }
    }

    /**
     * Executes a get operation based on the transaction isolation level.
     *
     * @param key the key to look up
     * @return the value
     */
    protected abstract V read(K key);

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    @Override
    public V put(K key, V value) {
        checkNotNull(value, ERROR_NULL_VALUE);

        V latest = get(key);
        writeCache.put(key, value);
        deleteSet.remove(key);
        return latest;
    }

    @Override
    public V remove(K key) {
        V latest = get(key);
        if (latest != null) {
            writeCache.remove(key);
            deleteSet.add(key);
        }
        return latest;
    }

    @Override
    public boolean remove(K key, V value) {
        checkNotNull(value, ERROR_NULL_VALUE);

        V latest = get(key);
        if (Objects.equal(value, latest)) {
            remove(key);
            return true;
        }
        return false;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkNotNull(oldValue, ERROR_NULL_VALUE);
        checkNotNull(newValue, ERROR_NULL_VALUE);

        V latest = get(key);
        if (Objects.equal(oldValue, latest)) {
            put(key, newValue);
            return true;
        }
        return false;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        checkNotNull(value, ERROR_NULL_VALUE);

        V latest = get(key);
        if (latest == null) {
            put(key, value);
        }
        return latest;
    }

    @Override
    public boolean hasPendingUpdates() {
        return records().findAny().isPresent();
    }

    @Override
    public CompletableFuture<Boolean> prepare() {
        return transaction.prepare(log());
    }

    @Override
    public CompletableFuture<Void> commit() {
        return transaction.commit();
    }

    @Override
    public CompletableFuture<Boolean> prepareAndCommit() {
        return transaction.prepareAndCommit(log());
    }

    @Override
    public CompletableFuture<Void> rollback() {
        return transaction.rollback();
    }

    /**
     * Returns a list of updates performed within this map partition.
     *
     * @return a list of map updates
     */
    protected List<MapRecord<K, V>> log() {
        return records().collect(Collectors.toList());
    }

    /**
     * Returns a stream of updates performed within this map partition.
     *
     * @return a stream of map updates
     */
    protected abstract Stream<MapRecord<K, V>> records();

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("backingMap", backingMap)
                .add("updates", log())
                .toString();
    }
}
