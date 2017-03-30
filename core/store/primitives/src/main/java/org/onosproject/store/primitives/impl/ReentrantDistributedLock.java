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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.onosproject.store.service.AsyncDistributedLock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reentrant distributed lock.
 */
public class ReentrantDistributedLock implements AsyncDistributedLock {
    private final AsyncDistributedLock asyncLock;
    private final AtomicInteger holdCount = new AtomicInteger();
    private volatile CompletableFuture<Boolean> lockFuture;

    public ReentrantDistributedLock(AsyncDistributedLock asyncLock) {
        this.asyncLock = checkNotNull(asyncLock);
    }

    @Override
    public String name() {
        return asyncLock.name();
    }

    @Override
    public CompletableFuture<Void> lock() {
        return doLock(() -> asyncLock.lock().thenApply(v -> true)).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Boolean> tryLock() {
        return doLock(() -> asyncLock.tryLock());
    }

    @Override
    public CompletableFuture<Boolean> tryLock(Duration timeout) {
        return doLock(() -> asyncLock.tryLock(timeout));
    }

    /**
     * Acquires a lock using the given supplier.
     */
    private synchronized CompletableFuture<Boolean> doLock(Supplier<CompletableFuture<Boolean>> supplier) {
        if (holdCount.incrementAndGet() == 1) {
            CompletableFuture<Boolean> future = supplier.get().whenComplete((locked, error) -> {
                if (error != null || !locked) {
                    holdCount.set(0);
                }
            });
            lockFuture = future;
            lockFuture.whenComplete((r, e) -> {
                synchronized (this) {
                    lockFuture = null;
                }
            });
            return future;
        } else if (lockFuture == null) {
            return CompletableFuture.completedFuture(true);
        }
        return lockFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> unlock() {
        if (holdCount.decrementAndGet() == 0) {
            return asyncLock.unlock();
        }
        return CompletableFuture.completedFuture(null);
    }
}
