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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.common.collect.Sets;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CommunicationStrategy;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.session.CopycatSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Copycat session that supports recovery.
 */
public class RecoveringCopycatSession implements CopycatSession {
    private final Logger log = LoggerFactory.getLogger(RecoveringCopycatSession.class);
    private final String name;
    private final String type;
    private final CommunicationStrategy communicationStrategy;
    private final RecoveringCopycatClient client;
    private CopycatSession session;
    private volatile CopycatSession.State state = State.CLOSED;
    private final Set<Consumer<State>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
    private final Map<String, Consumer> eventListeners = new ConcurrentHashMap<>();

    public RecoveringCopycatSession(String name, String type, CommunicationStrategy communicationStrategy, RecoveringCopycatClient client) {
        this.name = name;
        this.type = type;
        this.communicationStrategy = communicationStrategy;
        this.client = client;
        client.onStateChange(this::onClientStateChange);
        openSession();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public State state() {
        return state;
    }

    /**
     * Sets the session state.
     *
     * @param state the session state
     */
    private void setState(State state) {
        if (this.state != state) {
            log.debug("State changed: {}", state);
            this.state = state;
            stateChangeListeners.forEach(l -> context().execute(() -> l.accept(state)));
        }
    }

    @Override
    public Listener<State> onStateChange(Consumer<State> callback) {
        stateChangeListeners.add(callback);
        return new Listener<State>() {
            @Override
            public void accept(State state) {
                callback.accept(state);
            }

            @Override
            public void close() {
                stateChangeListeners.remove(callback);
            }
        };
    }

    @Override
    public ThreadContext context() {
        return session.context();
    }

    /**
     * Handles a client state change.
     *
     * @param state the changed client state
     */
    private void onClientStateChange(CopycatClient.State state) {
        // If the client state was changed to CONNECTED then reopen the session.
        switch (state) {
            case CONNECTED:
                openSession();
                setState(State.OPEN);
                break;
            case CLOSED:
                setState(State.CLOSED);
                break;
            default:
                break;
        }
    }

    /**
     * Opens the session.
     */
    private void openSession() {
        log.debug("Opening session");
        session = client.client.sessionBuilder()
                .withName(name)
                .withType(type)
                .withCommunicationStrategy(communicationStrategy)
                .build();
        eventListeners.forEach(session::onEvent);
        setState(State.OPEN);
    }

    @Override
    public <T> CompletableFuture<T> submit(Command<T> command) {
        return session.submit(command);
    }

    @Override
    public <T> CompletableFuture<T> submit(Query<T> query) {
        return session.submit(query);
    }

    @Override
    public Listener<Void> onEvent(String event, Runnable callback) {
        eventListeners.put(event, e -> callback.run());
        return session.onEvent(event, callback);
    }

    @Override
    public <T> Listener<T> onEvent(String event, Consumer<T> callback) {
        eventListeners.put(event, callback);
        return session.onEvent(event, callback);
    }

    @Override
    public boolean isOpen() {
        return state == CopycatSession.State.OPEN;
    }

    @Override
    public CompletableFuture<Void> close() {
        return session.close();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("type", type)
                .add("name", name)
                .add("state", state)
                .toString();
    }
}
