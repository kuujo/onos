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

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.Versioned;

/**
 * Snapshot based map participant.
 */
public class SnapshotMapParticipant<K, V> extends TransactionalMapParticipant<K, V> {
    private volatile boolean failed;

    public SnapshotMapParticipant(AsyncConsistentMap<K, V> backingMap, Transaction<MapRecord<K, V>> transaction) {
        super(backingMap, transaction);
    }

    @Override
    protected V read(K key) {
        Versioned<V> value = backingConsistentMap.get(key);
        if (value != null) {
            if (value.version() > lock.version()) {
                failed = true;
            }
            return value.value();
        }
        return null;
    }

    @Override
    public CompletableFuture<Boolean> prepare() {
        if (failed) {
            return CompletableFuture.completedFuture(false);
        } else {
            return super.prepare();
        }
    }

    @Override
    public CompletableFuture<Boolean> prepareAndCommit() {
        if (failed) {
            return CompletableFuture.completedFuture(true);
        } else {
            return super.prepareAndCommit();
        }
    }

    @Override
    protected Stream<MapRecord<K, V>> records() {
        return Stream.concat(deleteStream(), writeStream());
    }

    /**
     * Returns a transaction record stream for deleted keys.
     */
    private Stream<MapRecord<K, V>> deleteStream() {
        return deleteSet.stream()
                .map(key -> MapRecord.<K, V>newBuilder()
                        .withType(MapRecord.Type.REMOVE_IF_VERSION_MATCH)
                        .withKey(key)
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
