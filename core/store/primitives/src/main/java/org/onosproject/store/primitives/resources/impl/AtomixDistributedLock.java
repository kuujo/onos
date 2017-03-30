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
package org.onosproject.store.primitives.resources.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.atomix.concurrent.DistributedLock;
import io.atomix.resource.Resource;
import org.onosproject.store.service.AsyncDistributedLock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix distributed lock.
 * <p>
 * This implementation simply wraps an Atomix {@link DistributedLock}.
 */
public class AtomixDistributedLock implements AsyncDistributedLock {
    private final String name;
    private final DistributedLock distLock;
    private final Set<Consumer<Status>> statusChangeListeners = Sets.newCopyOnWriteArraySet();

    private final Function<Resource.State, Status> mapper = state -> {
        switch (state) {
            case CONNECTED:
                return Status.ACTIVE;
            case SUSPENDED:
                return Status.SUSPENDED;
            case CLOSED:
                return Status.INACTIVE;
            default:
                throw new IllegalStateException("Unknown state " + state);
        }
    };

    public AtomixDistributedLock(String name, DistributedLock distLock) {
        this.name = name;
        this.distLock = checkNotNull(distLock);
        distLock.onStateChange(this::handleStateChange);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public CompletableFuture<Void> lock() {
        return distLock.lock().thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Boolean> tryLock() {
        return distLock.tryLock().thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<Boolean> tryLock(Duration timeout) {
        return distLock.tryLock(timeout).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<Void> unlock() {
        return distLock.unlock();
    }

    @Override
    public void addStatusChangeListener(Consumer<Status> listener) {
        statusChangeListeners.add(listener);
    }

    @Override
    public void removeStatusChangeListener(Consumer<Status> listener) {
        statusChangeListeners.remove(listener);
    }

    @Override
    public Collection<Consumer<Status>> statusChangeListeners() {
        return ImmutableSet.copyOf(statusChangeListeners);
    }

    private void handleStateChange(Resource.State state) {
        statusChangeListeners().forEach(listener -> listener.accept(mapper.apply(state)));
    }
}
