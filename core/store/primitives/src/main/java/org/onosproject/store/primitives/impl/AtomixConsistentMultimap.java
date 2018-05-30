/*
 * Copyright 2018-present Open Networking Foundation
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import org.onosproject.store.service.AsyncConsistentMultimap;
import org.onosproject.store.service.MultimapEvent;
import org.onosproject.store.service.MultimapEventListener;
import org.onosproject.store.service.Versioned;

/**
 * Atomix consistent map.
 */
public class AtomixConsistentMultimap<K, V> implements AsyncConsistentMultimap<K, V> {
    private final io.atomix.core.multimap.AsyncConsistentMultimap<K, V> atomixMultimap;
    private final Map<MultimapEventListener<K, V>, io.atomix.core.multimap.MultimapEventListener<K, V>> listenerMap =
        Maps.newIdentityHashMap();

    public AtomixConsistentMultimap(io.atomix.core.multimap.AsyncConsistentMultimap<K, V> atomixMultimap) {
        this.atomixMultimap = atomixMultimap;
    }

    @Override
    public String name() {
        return atomixMultimap.name();
    }

    @Override
    public CompletableFuture<Integer> size() {
        return atomixMultimap.size();
    }

    @Override
    public CompletableFuture<Boolean> containsKey(K key) {
        return atomixMultimap.containsKey(key);
    }

    @Override
    public CompletableFuture<Boolean> containsValue(V value) {
        return atomixMultimap.containsValue(value);
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
        return atomixMultimap.isEmpty();
    }

    @Override
    public CompletableFuture<Boolean> containsEntry(K key, V value) {
        return atomixMultimap.containsEntry(key, value);
    }

    @Override
    public CompletableFuture<Boolean> put(K key, V value) {
        return atomixMultimap.put(key, value);
    }

    @Override
    public CompletableFuture<Boolean> remove(K key, V value) {
        return atomixMultimap.remove(key, value);
    }

    @Override
    public CompletableFuture<Boolean> removeAll(K key, Collection<? extends V> values) {
        return atomixMultimap.removeAll(key, values);
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends V>>> removeAll(K key) {
        return atomixMultimap.removeAll(key).thenApply(this::toVersioned);
    }

    @Override
    public CompletableFuture<Boolean> putAll(K key, Collection<? extends V> values) {
        return atomixMultimap.putAll(key, values);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Versioned<Collection<? extends V>>> replaceValues(K key, Collection<V> values) {
        return atomixMultimap.replaceValues(key, values).thenApply(this::toVersioned);
    }

    @Override
    public CompletableFuture<Void> clear() {
        return atomixMultimap.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Versioned<Collection<? extends V>>> get(K key) {
        return atomixMultimap.get(key).thenApply(this::toVersioned);
    }

    @Override
    public CompletableFuture<Set<K>> keySet() {
        return atomixMultimap.keySet();
    }

    @Override
    public CompletableFuture<Multiset<K>> keys() {
        return atomixMultimap.keys();
    }

    @Override
    public CompletableFuture<Multiset<V>> values() {
        return atomixMultimap.values();
    }

    @Override
    public CompletableFuture<Collection<Map.Entry<K, V>>> entries() {
        return atomixMultimap.entries();
    }

    @Override
    public CompletableFuture<Map<K, Collection<V>>> asMap() {
        return atomixMultimap.asMap();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(MultimapEventListener<K, V> listener, Executor executor) {
        io.atomix.core.multimap.MultimapEventListener<K, V> atomixListener = event ->
            listener.event(new MultimapEvent<K, V>(
                event.name(),
                event.key(),
                event.newValue(),
                event.oldValue()));
        listenerMap.put(listener, atomixListener);
        return atomixMultimap.addListener(atomixListener, executor);
    }

    @Override
    public CompletableFuture<Void> removeListener(MultimapEventListener<K, V> listener) {
        io.atomix.core.multimap.MultimapEventListener<K, V> atomixListener = listenerMap.remove(listener);
        if (atomixListener != null) {
            return atomixMultimap.removeListener(atomixListener);
        }
        return CompletableFuture.completedFuture(null);
    }

    private Versioned<Collection<? extends V>> toVersioned(io.atomix.utils.time.Versioned<Collection<? extends V>> versioned) {
        return versioned != null
            ? new Versioned<>(versioned.value(), versioned.version(), versioned.creationTime())
            : null;
    }
}
