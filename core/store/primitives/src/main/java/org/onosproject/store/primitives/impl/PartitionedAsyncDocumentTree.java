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

import org.onlab.util.Tools;
import org.onosproject.store.primitives.DistributedPrimitiveCreator;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.AsyncDocumentTree;
import org.onosproject.store.service.AsyncDocumentTreeNode;
import org.onosproject.store.service.DocumentPath;
import org.onosproject.store.service.DocumentTreeListener;
import org.onosproject.store.service.IllegalDocumentModificationException;
import org.onosproject.store.service.NoSuchDocumentPathException;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.Versioned;

import static com.google.common.base.Preconditions.checkState;

/**
 * Default document tree.
 */
public class PartitionedAsyncDocumentTree<V> implements AsyncDocumentTree<V> {
    private final DocumentPath rootPath;
    private final PartitionedAsyncDocumentTreeNode<V> root;
    private static final byte[] EMPTY = new byte[0];

    public PartitionedAsyncDocumentTree(
            String name,
            Serializer serializer,
            DistributedPrimitiveCreator primitiveCreator) {
        this.rootPath = DocumentPath.from(name);
        AsyncConsistentMap<String, Object> map =
                DistributedPrimitives.newCachingMap(
                        DistributedPrimitives.newNullableMap(primitiveCreator.newAsyncConsistentMap(name, serializer)));
        this.root = new PartitionedAsyncDocumentTreeNode<>(
                rootPath, new Versioned<>(null, 0), map, serializer, primitiveCreator);
    }

    @Override
    public String name() {
        return simpleName(rootPath);
    }

    @Override
    public DocumentPath root() {
        return rootPath;
    }

    @Override
    public CompletableFuture<Map<String, Versioned<V>>> getChildren(DocumentPath path) {
        return getNode(path)
                .thenCompose(node -> {
                    if (node != null) {
                        return node.getChildren();
                    }
                    return Tools.exceptionalFuture(new NoSuchDocumentPathException());
                });
    }

    @Override
    public CompletableFuture<Versioned<V>> get(DocumentPath path) {
        if (path.equals(rootPath)) {
            return CompletableFuture.completedFuture(root.value());
        }
        return getNode(path.parent())
                .thenCompose(parent -> parent != null ? parent.getChild(simpleName(path)) : null);
    }

    @Override
    public CompletableFuture<Versioned<V>> set(DocumentPath path, V value) {
        checkRootModification(path);
        return getNode(path.parent())
                .thenCompose(parent -> {
                    if (parent != null) {
                        return parent.setChild(simpleName(path), value);
                    } else {
                        return create(path, value).thenApply(v -> null);
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> create(DocumentPath path, V value) {
        checkRootModification(path);
        return getNode(path.parent())
                .thenCompose(parent -> {
                    if (parent == null) {
                        return Tools.exceptionalFuture(new IllegalDocumentModificationException());
                    }
                    return parent.addChild(simpleName(path), value == null ? EMPTY : value);
                });
    }

    @Override
    public CompletableFuture<Boolean> createRecursive(DocumentPath path, V value) {
        checkRootModification(path);
        DocumentPath parentPath = path.parent();
        return getNode(parentPath).thenCompose(parent -> {
            if (parent == null) {
                return createRecursive(parentPath, null).thenApply(v -> true);
            }
            return CompletableFuture.completedFuture(true);
        }).thenCompose(created -> {
            if (!created) {
                return CompletableFuture.completedFuture(false);
            }

            return getNode(parentPath)
                    .thenCompose(parent -> parent.addChild(simpleName(path), value));
        });
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, long version) {
        checkRootModification(path);
        return getNode(path.parent())
                .thenCompose(parent -> parent.replaceChild(simpleName(path), version, newValue));
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, V currentValue) {
        checkRootModification(path);
        if (Objects.equals(newValue, currentValue)) {
            return CompletableFuture.completedFuture(false);
        }
        return getNode(path.parent())
                .thenCompose(parent -> parent.replaceChild(simpleName(path), currentValue, newValue));
    }

    @Override
    public CompletableFuture<Versioned<V>> removeNode(DocumentPath path) {
        checkRootModification(path);
        return getNode(path.parent())
                .thenCompose(parent -> {
                    if (parent != null) {
                        CompletableFuture<Versioned<V>> future = new CompletableFuture<>();
                        parent.removeChild(simpleName(path)).whenComplete((value, error) -> {
                            if (error == null && value != null) {
                                future.complete(value);
                            } else {
                                future.completeExceptionally(new NoSuchDocumentPathException());
                            }
                        });
                        return future;
                    }
                    return Tools.exceptionalFuture(new NoSuchDocumentPathException());
                });
    }

    @Override
    public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeListener<V> listener) {
        return Tools.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> removeListener(DocumentTreeListener<V> listener) {
        return Tools.exceptionalFuture(new UnsupportedOperationException());
    }

    private CompletableFuture<PartitionedAsyncDocumentTreeNode<V>> getNode(DocumentPath path) {
        Iterator<String> pathElements = path.pathElements().iterator();
        checkState(rootPath.pathElements().get(0).equals(pathElements.next()), "Path should start with root");
        return getChild(root, pathElements);
    }

    private CompletableFuture<PartitionedAsyncDocumentTreeNode<V>> getChild(
            AsyncDocumentTreeNode<V> parent,
            Iterator<String> pathElements) {
        if (parent == null) {
            return CompletableFuture.completedFuture(null);
        } else if (pathElements.hasNext()) {
            return parent.child(pathElements.next()).thenCompose(child -> getChild(child, pathElements));
        } else {
            return CompletableFuture.completedFuture((PartitionedAsyncDocumentTreeNode<V>) parent);
        }
    }

    private String simpleName(DocumentPath path) {
        return path.pathElements().get(path.pathElements().size() - 1);
    }

    private void checkRootModification(DocumentPath path) {
        if (rootPath.equals(path)) {
            throw new IllegalDocumentModificationException();
        }
    }
}
