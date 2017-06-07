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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import org.onlab.util.KryoNamespace;
import org.onlab.util.Match;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeCommands.Clear;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeCommands.Get;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeCommands.GetChildren;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeCommands.Listen;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeCommands.Unlisten;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTreeCommands.Update;
import org.onosproject.store.primitives.resources.impl.DocumentTreeUpdateResult.Status;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.DocumentPath;
import org.onosproject.store.service.DocumentTree;
import org.onosproject.store.service.DocumentTreeEvent;
import org.onosproject.store.service.DocumentTreeEvent.Type;
import org.onosproject.store.service.IllegalDocumentModificationException;
import org.onosproject.store.service.NoSuchDocumentPathException;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.Versioned;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * State Machine for {@link AtomixDocumentTree} resource.
 */
public class AtomixDocumentTreeState extends StateMachine implements SessionListener, Snapshottable {

    private final Logger log = getLogger(getClass());
    private final Serializer serializer = Serializer.using(KryoNamespace.newBuilder()
            .register(KryoNamespaces.BASIC)
            .register(new com.esotericsoftware.kryo.Serializer<Listener>() {
                @Override
                public void write(Kryo kryo, Output output, Listener listener) {
                    output.writeLong(listener.session.id());
                    kryo.writeObject(output, listener.path);
                }

                @Override
                public Listener read(Kryo kryo, Input input, Class<Listener> type) {
                    return new Listener(sessions.session(input.readLong()),
                            kryo.readObjectOrNull(input, DocumentPath.class));
                }
            }, Listener.class)
            .register(Versioned.class)
            .register(DocumentPath.class)
            .register(new HashMap().keySet().getClass())
            .register(TreeMap.class)
            .register(new com.esotericsoftware.kryo.Serializer<DefaultDocumentTree>() {
                @Override
                public void write(Kryo kryo, Output output, DefaultDocumentTree object) {
                    kryo.writeObject(output, object.root);
                }

                @Override
                @SuppressWarnings("unchecked")
                public DefaultDocumentTree read(Kryo kryo, Input input, Class<DefaultDocumentTree> type) {
                    return new DefaultDocumentTree(versionCounter::incrementAndGet,
                            kryo.readObject(input, DefaultDocumentTreeNode.class));
                }
            }, DefaultDocumentTree.class)
            .register(DefaultDocumentTreeNode.class)
            .build());

    private final Map<Long, SessionListenCommits> listeners = new HashMap<>();
    private AtomicLong versionCounter = new AtomicLong(0);
    private final DocumentTree<byte[]> docTree = new DefaultDocumentTree<>(versionCounter::incrementAndGet);

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeLong(versionCounter.get());

        byte[] encodedListeners = serializer.encode(listeners.keySet());
        writer.writeInt(encodedListeners.length);
        writer.write(encodedListeners);

        byte[] encodedDocTree = serializer.encode(docTree);
        writer.writeInt(encodedDocTree.length);
        writer.write(encodedDocTree);
    }

    @Override
    public void install(SnapshotReader reader) {
        versionCounter = new AtomicLong(reader.readLong());
    }

    @Override
    protected void configure(StateMachineExecutor executor) {
        // Listeners
        executor.register(Listen.class, this::listen);
        executor.register(Unlisten.class, this::unlisten);
        // queries
        executor.register(Get.class, this::get);
        executor.register(GetChildren.class, this::getChildren);
        // commands
        executor.register(Update.class, this::update);
        executor.register(Clear.class, this::clear);
    }

    protected void listen(Commit<? extends Listen> commit) {
        Long sessionId = commit.session().id();
        listeners.computeIfAbsent(sessionId, k -> new SessionListenCommits())
                .add(new Listener(commit.session(), commit.operation().path()));
        commit.session().onStateChange(
                        state -> {
                            if (state == ServerSession.State.CLOSED
                                    || state == ServerSession.State.EXPIRED) {
                                closeListener(commit.session().id());
                            }
                        });
    }

    protected void unlisten(Commit<? extends Unlisten> commit) {
        Long sessionId = commit.session().id();
        SessionListenCommits listenCommits = listeners.get(sessionId);
        if (listenCommits != null) {
            listenCommits.remove(commit);
        }
    }

    protected Versioned<byte[]> get(Commit<? extends Get> commit) {
        try {
            Versioned<byte[]> value = docTree.get(commit.operation().path());
            return value == null ? null : value.map(node -> node == null ? null : node);
        } catch (IllegalStateException e) {
            return null;
        }
    }

    protected Map<String, Versioned<byte[]>> getChildren(Commit<? extends GetChildren> commit) {
        return docTree.getChildren(commit.operation().path());
    }

    protected DocumentTreeUpdateResult<byte[]> update(Commit<? extends Update> commit) {
        DocumentTreeUpdateResult<byte[]> result = null;
        DocumentPath path = commit.operation().path();
        boolean updated = false;
        Versioned<byte[]> currentValue = docTree.get(path);
        try {
            Match<Long> versionMatch = commit.operation().versionMatch();
            Match<byte[]> valueMatch = commit.operation().valueMatch();

            if (versionMatch.matches(currentValue == null ? null : currentValue.version())
                    && valueMatch.matches(currentValue == null ? null : currentValue.value())) {
                if (commit.operation().value() == null) {
                    docTree.removeNode(path);
                } else {
                    docTree.set(path, commit.operation().value().orElse(null));
                }
                updated = true;
            }
            Versioned<byte[]> newValue = updated ? docTree.get(path) : currentValue;
            Status updateStatus = updated
                    ? Status.OK : commit.operation().value() == null ? Status.INVALID_PATH : Status.NOOP;
            result = new DocumentTreeUpdateResult<>(path, updateStatus, newValue, currentValue);
        } catch (IllegalDocumentModificationException e) {
            result = DocumentTreeUpdateResult.illegalModification(path);
        } catch (NoSuchDocumentPathException e) {
            result = DocumentTreeUpdateResult.invalidPath(path);
        } catch (Exception e) {
            log.error("Failed to apply {} to state machine", commit.operation(), e);
            throw Throwables.propagate(e);
        }
        notifyListeners(path, result);
        return result;
    }

    protected void clear(Commit<? extends Clear> commit) {
        Queue<DocumentPath> toClearQueue = Queues.newArrayDeque();
        Map<String, Versioned<byte[]>> topLevelChildren = docTree.getChildren(DocumentPath.from("root"));
        toClearQueue.addAll(topLevelChildren.keySet()
                .stream()
                .map(name -> new DocumentPath(name, DocumentPath.from("root")))
                .collect(Collectors.toList()));
        while (!toClearQueue.isEmpty()) {
            DocumentPath path = toClearQueue.remove();
            Map<String, Versioned<byte[]>> children = docTree.getChildren(path);
            if (children.size() == 0) {
                docTree.removeNode(path);
            } else {
                children.keySet().forEach(name -> toClearQueue.add(new DocumentPath(name, path)));
                toClearQueue.add(path);
            }
        }
    }

    private void notifyListeners(DocumentPath path, DocumentTreeUpdateResult<byte[]> result) {
        if (result.status() != Status.OK) {
            return;
        }
        DocumentTreeEvent<byte[]> event =
                new DocumentTreeEvent<>(path,
                        result.created() ? Type.CREATED : result.newValue() == null ? Type.DELETED : Type.UPDATED,
                        Optional.ofNullable(result.newValue()),
                        Optional.ofNullable(result.oldValue()));

        listeners.values()
                 .stream()
                 .filter(l -> event.path().isDescendentOf(l.leastCommonAncestorPath()))
                 .forEach(listener -> listener.publish(AtomixDocumentTree.CHANGE_SUBJECT, Arrays.asList(event)));
    }

    @Override
    public void register(ServerSession session) {
    }

    @Override
    public void unregister(ServerSession session) {
    }

    @Override
    public void expire(ServerSession session) {
    }

    @Override
    public void close(ServerSession session) {
        closeListener(session.id());
    }

    private void closeListener(Long sessionId) {
        listeners.remove(sessionId);
    }

    private static class SessionListenCommits {
        private final List<Listener> listeners = Lists.newArrayList();
        private DocumentPath leastCommonAncestorPath;

        public void add(Listener listener) {
            listeners.add(listener);
            recomputeLeastCommonAncestor();
        }

        public void remove(Commit<? extends Unlisten> commit) {
            // Remove the first listen commit with path matching path in unlisten commit
            Iterator<Listener> iterator = listeners.iterator();
            while (iterator.hasNext()) {
                Listener listener = iterator.next();
                if (listener.path().equals(commit.operation().path())) {
                    iterator.remove();
                }
            }
            recomputeLeastCommonAncestor();
        }

        public DocumentPath leastCommonAncestorPath() {
            return leastCommonAncestorPath;
        }

        public <M> void publish(String topic, M message) {
            listeners.stream().findAny().ifPresent(listener -> listener.session().publish(topic, message));
        }

        private void recomputeLeastCommonAncestor() {
            this.leastCommonAncestorPath = DocumentPath.leastCommonAncestor(listeners.stream()
                    .map(Listener::path)
                    .collect(Collectors.toList()));
        }
    }

    private static class Listener {
        private final ServerSession session;
        private final DocumentPath path;

        public Listener(ServerSession session, DocumentPath path) {
            this.session = session;
            this.path = path;
        }

        public DocumentPath path() {
            return path;
        }

        public ServerSession session() {
            return session;
        }
    }
}
