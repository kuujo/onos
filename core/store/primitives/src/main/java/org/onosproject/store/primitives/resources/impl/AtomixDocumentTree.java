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

package org.onosproject.store.primitives.resources.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.protocols.raft.proxy.RaftProxy;
import org.onlab.util.KryoNamespace;
import org.onlab.util.Match;
import org.onlab.util.Tools;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.Get;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.GetChildren;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.Listen;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.Unlisten;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.Update;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.AsyncDocumentTree;
import org.onosproject.store.service.DocumentPath;
import org.onosproject.store.service.DocumentTreeEvent;
import org.onosproject.store.service.DocumentTreeListener;
import org.onosproject.store.service.IllegalDocumentModificationException;
import org.onosproject.store.service.NoSuchDocumentPathException;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.Versioned;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeEvents.CHANGE;
import static org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.ADD_LISTENER;
import static org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.CLEAR;
import static org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.GET;
import static org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.GET_CHILDREN;
import static org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.REMOVE_LISTENER;
import static org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeOperations.UPDATE;
import static org.onosproject.store.primitives.resources.impl.DocumentTreeUpdateResult.Status.ILLEGAL_MODIFICATION;
import static org.onosproject.store.primitives.resources.impl.DocumentTreeUpdateResult.Status.INVALID_PATH;
import static org.onosproject.store.primitives.resources.impl.DocumentTreeUpdateResult.Status.OK;

/**
 * Distributed resource providing the {@link AsyncDocumentTree} primitive.
 */
public class AtomixDocumentTree extends AbstractRaftPrimitive implements AsyncDocumentTree<byte[]> {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
            .register(KryoNamespaces.BASIC)
            .register(AtomixDocumentTreeOperations.NAMESPACE)
            .register(AtomixDocumentTreeEvents.NAMESPACE)
            .build());

    private final Map<DocumentTreeListener<byte[]>, InternalListener> eventListeners = new HashMap<>();

    public AtomixDocumentTree(RaftProxy proxy) {
        super(proxy);
        proxy.addStateChangeListener(state -> {
            if (state == RaftProxy.State.CONNECTED && isListening()) {
                proxy.submit(ADD_LISTENER, SERIALIZER::encode, new Listen());
            }
        });
        proxy.addEventListener(CHANGE, SERIALIZER::decode, this::processTreeUpdates);
    }

    @Override
    public Type primitiveType() {
        return Type.DOCUMENT_TREE;
    }

    @Override
    public CompletableFuture<Void> destroy() {
        return proxy.submit(CLEAR);
    }

    @Override
    public DocumentPath root() {
        return DocumentPath.from("root");
    }

    @Override
    public CompletableFuture<Map<String, Versioned<byte[]>>> getChildren(DocumentPath path) {
        return proxy.submit(GET_CHILDREN, SERIALIZER::encode, new GetChildren(checkNotNull(path)), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> get(DocumentPath path) {
        return proxy.submit(GET, SERIALIZER::encode, new Get(checkNotNull(path)), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> set(DocumentPath path, byte[] value) {
        return proxy.<Update, DocumentTreeUpdateResult<byte[]>>submit(UPDATE,
                SERIALIZER::encode,
                new Update(checkNotNull(path), Optional.ofNullable(value), Match.any(), Match.any()),
                SERIALIZER::decode)
                .thenCompose(result -> {
                    if (result.status() == INVALID_PATH) {
                        return Tools.exceptionalFuture(new NoSuchDocumentPathException());
                    } else if (result.status() == ILLEGAL_MODIFICATION) {
                        return Tools.exceptionalFuture(new IllegalDocumentModificationException());
                    } else {
                        return CompletableFuture.completedFuture(result);
                    }
                }).thenApply(result -> result.oldValue());
    }

    @Override
    public CompletableFuture<Boolean> create(DocumentPath path, byte[] value) {
        return createInternal(path, value)
                .thenCompose(status -> {
                    if (status == ILLEGAL_MODIFICATION) {
                        return Tools.exceptionalFuture(new IllegalDocumentModificationException());
                    }
                    return CompletableFuture.completedFuture(true);
                });
    }

    @Override
    public CompletableFuture<Boolean> createRecursive(DocumentPath path, byte[] value) {
        return createInternal(path, value)
                .thenCompose(status -> {
                    if (status == ILLEGAL_MODIFICATION) {
                        return createRecursive(path.parent(), null)
                                .thenCompose(r -> createInternal(path, value).thenApply(v -> true));
                    }
                    return CompletableFuture.completedFuture(status == OK);
                });
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, byte[] newValue, long version) {
        return proxy.<Update, DocumentTreeUpdateResult<byte[]>>submit(UPDATE,
                SERIALIZER::encode,
                new Update(checkNotNull(path),
                        Optional.ofNullable(newValue),
                        Match.any(),
                        Match.ifValue(version)), SERIALIZER::decode)
                .thenApply(result -> result.updated());
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, byte[] newValue, byte[] currentValue) {
        return proxy.<Update, DocumentTreeUpdateResult<byte[]>>submit(UPDATE,
                SERIALIZER::encode,
                new Update(checkNotNull(path),
                        Optional.ofNullable(newValue),
                        Match.ifValue(currentValue),
                        Match.any()),
                SERIALIZER::decode)
                .thenCompose(result -> {
                    if (result.status() == INVALID_PATH) {
                        return Tools.exceptionalFuture(new NoSuchDocumentPathException());
                    } else if (result.status() == ILLEGAL_MODIFICATION) {
                        return Tools.exceptionalFuture(new IllegalDocumentModificationException());
                    } else {
                        return CompletableFuture.completedFuture(result);
                    }
                }).thenApply(result -> result.updated());
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> removeNode(DocumentPath path) {
        if (path.equals(DocumentPath.from("root"))) {
            return Tools.exceptionalFuture(new IllegalDocumentModificationException());
        }
        return proxy.<Update, DocumentTreeUpdateResult<byte[]>>submit(UPDATE,
                SERIALIZER::encode,
                new Update(checkNotNull(path), null, Match.any(), Match.ifNotNull()),
                SERIALIZER::decode)
                .thenCompose(result -> {
                    if (result.status() == INVALID_PATH) {
                        return Tools.exceptionalFuture(new NoSuchDocumentPathException());
                    } else if (result.status() == ILLEGAL_MODIFICATION) {
                        return Tools.exceptionalFuture(new IllegalDocumentModificationException());
                    } else {
                        return CompletableFuture.completedFuture(result);
                    }
                }).thenApply(result -> result.oldValue());
    }

    @Override
    public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeListener<byte[]> listener) {
        checkNotNull(path);
        checkNotNull(listener);
        InternalListener internalListener = new InternalListener(path, listener, MoreExecutors.directExecutor());
        // TODO: Support API that takes an executor
        if (!eventListeners.containsKey(listener)) {
            return proxy.submit(ADD_LISTENER, SERIALIZER::encode, new Listen(path))
                    .thenRun(() -> eventListeners.put(listener, internalListener));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeListener(DocumentTreeListener<byte[]> listener) {
        checkNotNull(listener);
        InternalListener internalListener = eventListeners.remove(listener);
        if  (internalListener != null && eventListeners.isEmpty()) {
            return proxy.submit(REMOVE_LISTENER, SERIALIZER::encode, new Unlisten(internalListener.path))
                    .thenApply(v -> null);
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<DocumentTreeUpdateResult.Status> createInternal(DocumentPath path, byte[] value) {
        return proxy.<Update, DocumentTreeUpdateResult<byte[]>>submit(UPDATE,
                SERIALIZER::encode,
                new Update(checkNotNull(path), Optional.ofNullable(value), Match.any(), Match.ifNull()),
                SERIALIZER::decode)
                .thenApply(result -> result.status());
    }

    private boolean isListening() {
        return !eventListeners.isEmpty();
    }

    private void processTreeUpdates(List<DocumentTreeEvent<byte[]>> events) {
        events.forEach(event -> eventListeners.values().forEach(listener -> listener.event(event)));
    }

    private class InternalListener implements DocumentTreeListener<byte[]> {

        private final DocumentPath path;
        private final DocumentTreeListener<byte[]> listener;
        private final Executor executor;

        public InternalListener(DocumentPath path, DocumentTreeListener<byte[]> listener, Executor executor) {
            this.path = path;
            this.listener = listener;
            this.executor = executor;
        }

        @Override
        public void event(DocumentTreeEvent<byte[]> event) {
            if (event.path().isDescendentOf(path)) {
                executor.execute(() -> listener.event(event));
            }
        }
    }
}