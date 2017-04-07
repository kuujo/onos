/*
 * Copyright 2016-present Open Networking Laboratory
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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.onosproject.store.primitives.DistributedPrimitiveCreator;
import org.onosproject.store.primitives.MapUpdate;
import org.onosproject.store.primitives.TransactionId;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.CommitStatus;
import org.onosproject.store.service.IsolationLevel;
import org.onosproject.store.service.LockMode;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.TransactionContext;
import org.onosproject.store.service.TransactionLog;
import org.onosproject.store.service.TransactionalMap;
import org.onosproject.store.service.TransactionalSet;
import org.onosproject.utils.MeteringAgent;

/**
 * Default implementation of transaction context.
 */
public class DefaultTransactionContext implements TransactionContext {

    private final AtomicBoolean isOpen = new AtomicBoolean(false);
    private final DistributedPrimitiveCreator creator;
    private final TransactionId transactionId;
    private final LockMode lockMode;
    private final IsolationLevel isolationLevel;
    private final TransactionCoordinator transactionCoordinator;
    private final Map<String, DefaultTransactionalMap> maps = Maps.newConcurrentMap();
    private final MeteringAgent monitor;

    public DefaultTransactionContext(TransactionId transactionId,
            LockMode lockMode,
            IsolationLevel isolationLevel,
            DistributedPrimitiveCreator creator,
            TransactionCoordinator transactionCoordinator) {
        this.transactionId = transactionId;
        this.lockMode = lockMode;
        this.isolationLevel = isolationLevel;
        this.creator = creator;
        this.transactionCoordinator = transactionCoordinator;
        this.monitor = new MeteringAgent("transactionContext", "*", true);
    }

    @Override
    public String name() {
        return transactionId.toString();
    }

    @Override
    public TransactionId transactionId() {
        return transactionId;
    }

    @Override
    public boolean isOpen() {
        return isOpen.get();
    }

    @Override
    public void begin() {
        if (!isOpen.compareAndSet(false, true)) {
            throw new IllegalStateException("TransactionContext is already open");
        }
    }

    @Override
    public CompletableFuture<CommitStatus> commit() {
        final MeteringAgent.Context timer = monitor.startTimer("commit");
        transaction.commit();
        final Collection<TransactionLog> transactionLogs = maps.values().stream()
                .map(map -> map.transactionLog)
                .collect(Collectors.toList());
        return transactionCoordinator.commit(transactionId, transactionLogs)
                                     .whenComplete((r, e) -> timer.stop(e));
    }

    @Override
    public void abort() {
        isOpen.set(false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> TransactionalMap<K, V> getTransactionalMap(String mapName,
            Serializer serializer) {
        return maps.computeIfAbsent(mapName, name -> {
            TransactionLog<MapUpdate<K, V>> txLog = new TransactionLog<>();
            // TODO: Reuse a caching AsyncConsistentMap for IsolationLevel.REPEATABLE_READ
            AsyncConsistentMap<K, V> asyncMap = DistributedPrimitives.newMeteredMap(creator.newAsyncConsistentMap(mapName, serializer));
            return new DefaultTransactionalMap<>(name, txLog, asyncMap, serializer);
        });
    }

    @Override
    public <T> TransactionalSet<T> getTransactionalSet(String setName, Serializer serializer) {
        // The set will wrap a TransactionalMap that's registered as the participant.
        return new DefaultTransactionalSet<>(getTransactionalMap(setName, serializer));
    }
}