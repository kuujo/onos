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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.atomix.catalyst.concurrent.BlockingFuture;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;
import org.onosproject.store.service.AsyncDistributedLock;

import static org.onosproject.store.primitives.resources.impl.AtomixDistributedLockCommands.Lock;
import static org.onosproject.store.primitives.resources.impl.AtomixDistributedLockCommands.LockEvent;
import static org.onosproject.store.primitives.resources.impl.AtomixDistributedLockCommands.Unlock;

/**
 * Atomix distributed lock.
 */
@ResourceTypeInfo(id = -158, factory = AtomixDistributedLockFactory.class)
public class AtomixDistributedLock extends AbstractResource<AtomixDistributedLock> implements AsyncDistributedLock {
    private final Set<Consumer<Status>> statusChangeListeners = Sets.newCopyOnWriteArraySet();
    private final Map<Integer, CompletableFuture<Long>> lockFutures = new ConcurrentHashMap<>();
    private final AtomicInteger id = new AtomicInteger();
    private volatile int lock;

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

    public AtomixDistributedLock(CopycatClient client, Properties options) {
        super(client, options);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public CompletableFuture<AtomixDistributedLock> open() {
        return super.open().thenApply(result -> {
            onStateChange(this::handleStateChange);
            client.onEvent("lock", this::handleEvent);
            client.onEvent("fail", this::handleFail);
            return result;
        });
    }

    /**
     * Handles a received lock event.
     */
    private void handleEvent(LockEvent event) {
        CompletableFuture<Long> future = lockFutures.remove(event.id());
        if (future != null) {
            this.lock = event.id();
            future.complete(event.version());
        }
    }

    /**
     * Handles a received failure event.
     */
    private void handleFail(LockEvent event) {
        CompletableFuture<Long> future = lockFutures.remove(event.id());
        if (future != null) {
            future.complete(null);
        }
    }

    @Override
    public CompletableFuture<Long> lock() {
        CompletableFuture<Long> future = new BlockingFuture<>();
        int id = this.id.incrementAndGet();
        lockFutures.put(id, future);
        client.submit(new Lock(id, -1)).whenComplete((result, error) -> {
            if (error != null) {
                lockFutures.remove(id);
                future.completeExceptionally(error);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Long> tryLock() {
        CompletableFuture<Long> future = new BlockingFuture<>();
        int id = this.id.incrementAndGet();
        lockFutures.put(id, future);
        client.submit(new Lock(id, 0)).whenComplete((result, error) -> {
            if (error != null) {
                lockFutures.remove(id);
                future.completeExceptionally(error);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Long> tryLock(Duration timeout) {
        CompletableFuture<Long> future = new BlockingFuture<>();
        int id = this.id.incrementAndGet();
        lockFutures.put(id, future);
        client.submit(new Lock(id, timeout.toMillis())).whenComplete((result, error) -> {
            if (error != null) {
                lockFutures.remove(id);
                future.completeExceptionally(error);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> unlock() {
        int lock = this.lock;
        if (lock != 0) {
            return client.submit(new Unlock(lock));
        }
        return CompletableFuture.completedFuture(null);
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
