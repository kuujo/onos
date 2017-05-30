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

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.collect.Sets;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.CopycatMetadata;
import io.atomix.copycat.client.session.CopycatSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Copycat client that recovers sessions that are lost.
 */
public class RecoveringCopycatClient implements CopycatClient {
    private static final long DEFAULT_RETRY_MILLIS = 100;

    private final Logger log = LoggerFactory.getLogger(RecoveringCopycatClient.class);
    private final CopycatClient.Builder clientBuilder;
    protected CopycatClient client;
    private final Set<Consumer<State>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
    private final AtomicBoolean open = new AtomicBoolean();

    public RecoveringCopycatClient(Builder clientBuilder) {
        this.clientBuilder = checkNotNull(clientBuilder);
        client = clientBuilder.build();
        client.onStateChange(this::onStateChange);
    }

    /**
     * Resets the internal client.
     */
    private void recover(Collection<Address> servers, long retryAfter) {
        log.warn("Raft client failed");
        client = clientBuilder.build();
        client.onStateChange(this::onStateChange);
        client.connect(servers).whenComplete((result, error) -> {
            if (error != null) {
                log.warn("Failed to connect to the cluster: {}", error);
                client.context().schedule(Duration.ofMillis(retryAfter), () -> {
                    if (open.get()) {
                        recover(servers, Math.min(retryAfter, 5000));
                    }
                });
            }
        });
    }

    /**
     * Handles a client state change.
     *
     * @param state The client state change to handle.
     */
    private void onStateChange(State state) {
        stateChangeListeners.forEach(l -> l.accept(state));
        log.debug("State changed: {}", state);
        if (state == State.CLOSED && open.get()) {
            recover(client.metadata().servers(), DEFAULT_RETRY_MILLIS);
        }
    }

    @Override
    public State state() {
        return client.state();
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
    public CopycatMetadata metadata() {
        return client.metadata();
    }

    @Override
    public ThreadContext context() {
        return client.context();
    }

    @Override
    public Serializer serializer() {
        return client.serializer();
    }

    @Override
    public CopycatSession.Builder sessionBuilder() {
        return new CopycatSession.Builder() {
            @Override
            public CopycatSession build() {
                return new RecoveringCopycatSession(name, type, communicationStrategy, RecoveringCopycatClient.this);
            }
        };
    }

    @Override
    public CompletableFuture<CopycatClient> connect(Collection<Address> members) {
        if (open.compareAndSet(false, true)) {
            return client.connect(members);
        }
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public CompletableFuture<Void> close() {
        if (open.compareAndSet(true, false)) {
            return client.close();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("delegate", client)
                .toString();
    }
}
