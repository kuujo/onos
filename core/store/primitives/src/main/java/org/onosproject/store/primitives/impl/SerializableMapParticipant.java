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

import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.Versioned;

/**
 * Serializable map participant.
 */
public class SerializableMapParticipant<K, V> extends TransactionalMapParticipant<K, V> {
    private final Map<K, Versioned<V>> readCache = Maps.newConcurrentMap();

    public SerializableMapParticipant(AsyncConsistentMap<K, V> backingMap, Transaction<MapRecord<K, V>> transaction) {
        super(backingMap, transaction);
    }

    @Override
    protected V read(K key) {
        return readCache.computeIfAbsent(key, k -> backingConsistentMap.getOrDefault(key, null)).value();
    }

    @Override
    protected Stream<MapRecord<K, V>> records() {
        return Stream.concat(readStream(), Stream.concat(deleteStream(), writeStream()));
    }

    /**
     * Returns a transaction record stream for read keys.
     */
    private Stream<MapRecord<K, V>> readStream() {
        return readCache.keySet().stream()
                .filter(key -> !deleteSet.contains(key) && !writeCache.containsKey(key))
                .map(key -> MapRecord.<K, V>newBuilder()
                        .withType(MapRecord.Type.LOCK)
                        .withKey(key)
                        .withCurrentVersion(lock.version())
                        .build());
    }

    /**
     * Returns a transaction record stream for deleted keys.
     */
    private Stream<MapRecord<K, V>> deleteStream() {
        return deleteSet.stream()
                .map(key -> Pair.of(key, readCache.get(key)))
                .filter(pair -> pair.getRight() != null)
                .map(pair -> MapRecord.<K, V>newBuilder()
                        .withType(MapRecord.Type.REMOVE_IF_VERSION_MATCH)
                        .withKey(pair.getLeft())
                        .withCurrentVersion(lock.version())
                        .build());
    }

    /**
     * Returns a transaction record stream for updated keys.
     */
    private Stream<MapRecord<K, V>> writeStream() {
        return writeCache.entrySet().stream()
                .map(entry -> MapRecord.<K, V>newBuilder()
                        .withType(MapRecord.Type.PUT_IF_VERSION_MATCH)
                        .withKey(entry.getKey())
                        .withCurrentVersion(lock.version())
                        .withValue(entry.getValue())
                        .build());
    }
}
