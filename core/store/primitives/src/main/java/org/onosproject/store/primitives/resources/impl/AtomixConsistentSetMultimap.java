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

import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import io.atomix.copycat.client.session.CopycatSession;
import org.onosproject.store.service.AsyncConsistentMultimap;
import org.onosproject.store.service.MultimapEvent;
import org.onosproject.store.service.MultimapEventListener;
import org.onosproject.store.service.Versioned;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Clear;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.ContainsEntry;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.ContainsKey;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.ContainsValue;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Entries;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Get;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.IsEmpty;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.KeySet;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Keys;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Listen;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.MultiRemove;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Put;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.RemoveAll;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Replace;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Size;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Unlisten;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMultimapCommands.Values;


/**
 * Set based implementation of the {@link AsyncConsistentMultimap}.
 * <p>
 * Note: this implementation does not allow null entries or duplicate entries.
 */
public class AtomixConsistentSetMultimap
        extends AbstractCopycatPrimitive
        implements AsyncConsistentMultimap<String, byte[]> {

    private final Map<MultimapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

    public static final String CHANGE_SUBJECT = "multimapChangeEvents";

    public AtomixConsistentSetMultimap(CopycatSession session) {
        super(session);
        session.onEvent(CHANGE_SUBJECT, this::handleEvent);
        session.onStateChange(state -> {
            if (state == CopycatSession.State.OPEN && isListening()) {
                session.submit(new Listen());
            }
        });
    }

    private void handleEvent(List<MultimapEvent<String, byte[]>> events) {
        events.forEach(event ->
                mapEventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event))));
    }

    @Override
    public CompletableFuture<Integer> size() {
        return session.submit(new Size());
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
        return session.submit(new IsEmpty());
    }

    @Override
    public CompletableFuture<Boolean> containsKey(String key) {
        return session.submit(new ContainsKey(key));
    }

    @Override
    public CompletableFuture<Boolean> containsValue(byte[] value) {
        return session.submit(new ContainsValue(value));
    }

    @Override
    public CompletableFuture<Boolean> containsEntry(String key, byte[] value) {
        return session.submit(new ContainsEntry(key, value));
    }

    @Override
    public CompletableFuture<Boolean> put(String key, byte[] value) {
        return session.submit(new Put(key, Lists.newArrayList(value), null));
    }

    @Override
    public CompletableFuture<Boolean> remove(String key, byte[] value) {
        return session.submit(new MultiRemove(key,
                                             Lists.newArrayList(value),
                                             null));
    }

    @Override
    public CompletableFuture<Boolean> removeAll(String key, Collection<? extends byte[]> values) {
        return session.submit(new MultiRemove(key, (Collection<byte[]>) values, null));
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends byte[]>>> removeAll(String key) {
        return session.submit(new RemoveAll(key, null));
    }

    @Override
    public CompletableFuture<Boolean> putAll(
            String key, Collection<? extends byte[]> values) {
        return session.submit(new Put(key, values, null));
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends byte[]>>> replaceValues(
            String key, Collection<byte[]> values) {
        return session.submit(new Replace(key, values, null));
    }

    @Override
    public CompletableFuture<Void> clear() {
        return session.submit(new Clear());
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends byte[]>>> get(String key) {
        return session.submit(new Get(key));
    }

    @Override
    public CompletableFuture<Set<String>> keySet() {
        return session.submit(new KeySet());
    }

    @Override
    public CompletableFuture<Multiset<String>> keys() {
        return session.submit(new Keys());
    }

    @Override
    public CompletableFuture<Multiset<byte[]>> values() {
        return session.submit(new Values());
    }

    @Override
    public CompletableFuture<Collection<Map.Entry<String, byte[]>>> entries() {
        return session.submit(new Entries());
    }

    @Override
    public CompletableFuture<Void> addListener(MultimapEventListener<String, byte[]> listener, Executor executor) {
        if (mapEventListeners.isEmpty()) {
            return session.submit(new Listen()).thenRun(() -> mapEventListeners.put(listener, executor));
        } else {
            mapEventListeners.put(listener, executor);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<Void> removeListener(MultimapEventListener<String, byte[]> listener) {
        if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
            return session.submit(new Unlisten()).thenApply(v -> null);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Map<String, Collection<byte[]>>> asMap() {
        throw new UnsupportedOperationException("Expensive operation.");
    }

    @Override
    public String name() {
        return null;
    }

    /**
     * Helper to check if there was a lock based issue.
     * @param status the status of an update result
     */
    private void throwIfLocked(MapEntryUpdateResult.Status status) {
        if (status == MapEntryUpdateResult.Status.WRITE_LOCK) {
            throw new ConcurrentModificationException("Cannot update map: " +
                                                      "Another transaction " +
                                                      "in progress");
        }
    }

    private boolean isListening() {
        return !mapEventListeners.isEmpty();
    }
}
