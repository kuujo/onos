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
import java.util.function.Consumer;

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.session.CopycatSession;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Copycat session that delegates to another session.
 */
public class DelegatingCopycatSession implements CopycatSession {
    protected final CopycatSession delegate;

    public DelegatingCopycatSession(CopycatSession delegate) {
        this.delegate = delegate;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public String type() {
        return delegate.type();
    }

    @Override
    public State state() {
        return delegate.state();
    }

    @Override
    public Listener<State> onStateChange(Consumer<State> callback) {
        return delegate.onStateChange(callback);
    }

    @Override
    public ThreadContext context() {
        return delegate.context();
    }

    @Override
    public <T> CompletableFuture<T> submit(Command<T> command) {
        return delegate.submit(command);
    }

    @Override
    public <T> CompletableFuture<T> submit(Query<T> query) {
        return delegate.submit(query);
    }

    @Override
    public Listener<Void> onEvent(String event, Runnable callback) {
        return delegate.onEvent(event, callback);
    }

    @Override
    public <T> Listener<T> onEvent(String event, Consumer<T> callback) {
        return delegate.onEvent(event, callback);
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public CompletableFuture<Void> close() {
        return delegate.close();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("type", type())
                .add("name", name())
                .add("state", state())
                .toString();
    }
}
