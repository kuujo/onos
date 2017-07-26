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

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.onosproject.store.primitives.DistributedPrimitiveCreator;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.AsyncDocumentTreeNode;
import org.onosproject.store.service.DocumentPath;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.Versioned;

/**
 * Default document tree.
 */
public class PartitionedAsyncDocumentTreeNode<V> implements AsyncDocumentTreeNode<V> {
    private final DocumentPath path;
    private final Versioned<V> value;
    private final AsyncConsistentMap<String, Object> map;
    private final Serializer serializer;
    private final DistributedPrimitiveCreator primitiveCreator;
    private final LoadingCache<String, AsyncConsistentMap<String, Object>> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, AsyncConsistentMap<String, Object>>() {
                @Override
                public AsyncConsistentMap<String, Object> load(String key) throws Exception {
                    return DistributedPrimitives.newCachingMap(
                            DistributedPrimitives.newNullableMap(
                                    primitiveCreator.newAsyncConsistentMap(key, serializer)));
                }
            });

    public PartitionedAsyncDocumentTreeNode(
            DocumentPath path,
            Versioned<V> value,
            AsyncConsistentMap<String, Object> map,
            Serializer serializer,
            DistributedPrimitiveCreator primitiveCreator) {
        this.path = path;
        this.value = value;
        this.map = map;
        this.serializer = serializer;
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public DocumentPath path() {
        return path;
    }

    @Override
    public Versioned<V> value() {
        return value;
    }

    @SuppressWarnings("unchecked")
    CompletableFuture<Map<String, Versioned<V>>> getChildren() {
        return map.entrySet().thenApply(entries ->
                ImmutableMap.<String, Versioned<V>>builder()
                        .putAll((Iterable) entries)
                        .build());
    }

    @SuppressWarnings("unchecked")
    CompletableFuture<Versioned<V>> getChild(String key) {
        return (CompletableFuture) map.get(key);
    }

    @SuppressWarnings("unchecked")
    CompletableFuture<Versioned<V>> setChild(String key, V value) {
        return (CompletableFuture) map.put(key, value);
    }

    @SuppressWarnings("unchecked")
    CompletableFuture<Boolean> addChild(String key, Object value) {
        return map.putIfAbsent(key, value).thenApply(Objects::isNull);
    }

    CompletableFuture<Boolean> replaceChild(String key, long version, V newValue) {
        return map.replace(key, version, newValue);
    }

    CompletableFuture<Boolean> replaceChild(String key, V oldValue, V newValue) {
        return map.replace(key, oldValue, newValue);
    }

    @SuppressWarnings("unchecked")
    CompletableFuture<Versioned<V>> removeChild(String key) {
        return (CompletableFuture) map.remove(key);
    }

    private AsyncConsistentMap<String, Object> childMap(String path) {
        return cache.getUnchecked(path);
    }

    private DocumentPath childPath(String child) {
        return DocumentPath.from(path.pathElements(), child);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Iterator<AsyncDocumentTreeNode<V>>> children() {
        return map.entrySet()
                .thenApply(entries -> entries.stream()
                        .<AsyncDocumentTreeNode<V>>map(entry -> {
                            DocumentPath childPath = childPath(entry.getKey());
                            return new PartitionedAsyncDocumentTreeNode<V>(
                                    childPath,
                                    (Versioned<V>) entry.getValue(),
                                    childMap(childPath.toString()),
                                    serializer,
                                    primitiveCreator);
                        })
                        .collect(Collectors.toList())
                        .iterator());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<AsyncDocumentTreeNode<V>> child(String relativePath) {
        return map.get(relativePath).thenApply(value -> {
            if (value != null) {
                return new PartitionedAsyncDocumentTreeNode<>(
                        childPath(relativePath),
                        (Versioned<V>) value,
                        childMap(relativePath),
                        serializer,
                        primitiveCreator);
            }
            return null;
        });
    }

    private String simpleName(DocumentPath path) {
        return path.pathElements().get(path.pathElements().size() - 1);
    }
}
