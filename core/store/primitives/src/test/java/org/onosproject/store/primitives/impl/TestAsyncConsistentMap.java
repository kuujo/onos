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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.onosproject.store.primitives.MapUpdate;
import org.onosproject.store.primitives.TransactionId;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.MapEventListener;
import org.onosproject.store.service.TransactionLog;
import org.onosproject.store.service.Version;
import org.onosproject.store.service.Versioned;

class TestAsyncConsistentMap<K, V> implements AsyncConsistentMap<K, V> {
    private final Map<K, Versioned<V>> map = new HashMap<>();
    private final AtomicLong version = new AtomicLong();

    @Override
    public String name() {
        return null;
    }

    @Override
    public Type primitiveType() {
        return Type.CONSISTENT_MAP;
    }

    private long nextVersion() {
        return version.incrementAndGet();
    }

    @Override
    public CompletableFuture<Integer> size() {
        return CompletableFuture.completedFuture(map.size());
    }

    @Override
    public CompletableFuture<Boolean> containsKey(K key) {
        return CompletableFuture.completedFuture(map.containsKey(key));
    }

    @Override
    public CompletableFuture<Boolean> containsValue(V value) {
        return CompletableFuture.completedFuture(map.values().stream()
                .filter(v -> Objects.equals(v.value(), value))
                .findFirst()
                .isPresent());
    }

    @Override
    public CompletableFuture<Versioned<V>> get(K key) {
        return CompletableFuture.completedFuture(map.get(key));
    }

    @Override
    public CompletableFuture<Versioned<V>> getOrDefault(K key, V defaultValue) {
        return CompletableFuture.completedFuture(map.getOrDefault(key, new Versioned<>(defaultValue, 0)));
    }

    @Override
    public CompletableFuture<Versioned<V>> computeIf(K key, Predicate<? super V> condition, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Versioned<V>> put(K key, V value) {
        return CompletableFuture.completedFuture(map.put(key, new Versioned<>(value, nextVersion())));
    }

    @Override
    public CompletableFuture<Versioned<V>> putAndGet(K key, V value) {
        return put(key, value);
    }

    @Override
    public CompletableFuture<Versioned<V>> remove(K key) {
        return CompletableFuture.completedFuture(map.remove(key));
    }

    @Override
    public CompletableFuture<Void> clear() {
        map.clear();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Set<K>> keySet() {
        return CompletableFuture.completedFuture(map.keySet());
    }

    @Override
    public CompletableFuture<Collection<Versioned<V>>> values() {
        return CompletableFuture.completedFuture(map.values());
    }

    @Override
    public CompletableFuture<Set<Map.Entry<K, Versioned<V>>>> entrySet() {
        return CompletableFuture.completedFuture(map.entrySet());
    }

    @Override
    public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value) {
        return CompletableFuture.completedFuture(map.computeIfAbsent(key, k -> new Versioned<>(value, nextVersion())));
    }

    @Override
    public CompletableFuture<Boolean> remove(K key, V value) {
        Versioned<V> versioned = map.get(key);
        if ((versioned == null && value == null)
                || (versioned != null && Objects.equals(versioned.value(), value))) {
            map.remove(key);
            return CompletableFuture.completedFuture(true);
        }
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> remove(K key, long version) {
        Versioned<V> versioned = map.get(key);
        if ((versioned == null && version == 0)
                || (versioned != null && versioned.version() == version)) {
            map.remove(key);
            return CompletableFuture.completedFuture(true);
        }
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Versioned<V>> replace(K key, V value) {
        Versioned<V> newVersioned = new Versioned<>(value, nextVersion());
        Versioned<V> versioned = map.put(key, newVersioned);
        if (versioned != null) {
            return CompletableFuture.completedFuture(versioned);
        } else {
            return CompletableFuture.completedFuture(newVersioned);
        }
    }

    @Override
    public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
        Versioned<V> versioned = map.get(key);
        if ((versioned == null && oldValue == null)
                || (versioned != null && Objects.equals(versioned.value(), oldValue))) {
            map.put(key, new Versioned<>(newValue, nextVersion()));
            return CompletableFuture.completedFuture(true);
        }
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue) {
        Versioned<V> versioned = map.get(key);
        if ((versioned == null && oldVersion == 0)
                || versioned != null && versioned.version() == oldVersion) {
            map.put(key, new Versioned<>(newValue, nextVersion()));
            return CompletableFuture.completedFuture(true);
        }
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Void> addListener(MapEventListener<K, V> listener, Executor executor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> removeListener(MapEventListener<K, V> listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Version> begin(TransactionId transactionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<K, V>> transactionLog) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> prepareAndCommit(TransactionLog<MapUpdate<K, V>> transactionLog) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
        throw new UnsupportedOperationException();
    }
}