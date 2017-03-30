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
    private volatile CompletableFuture<Long> lockFuture;
    private volatile Long version;

    public ReentrantDistributedLock(AsyncDistributedLock asyncLock) {
        this.asyncLock = checkNotNull(asyncLock);
    }

    @Override
    public String name() {
        return asyncLock.name();
    }

    @Override
    public CompletableFuture<Long> lock() {
        return doLock(asyncLock::lock).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Long> tryLock() {
        return doLock(asyncLock::tryLock);
    }

    @Override
    public CompletableFuture<Long> tryLock(Duration timeout) {
        return doLock(() -> asyncLock.tryLock(timeout));
    }

    /**
     * Acquires a lock using the given supplier.
     */
    private synchronized CompletableFuture<Long> doLock(Supplier<CompletableFuture<Long>> supplier) {
        if (holdCount.incrementAndGet() == 1) {
            CompletableFuture<Long> future = supplier.get().whenComplete((locked, error) -> {
                if (error != null || locked == null) {
                    holdCount.set(0);
                }
            });
            lockFuture = future;
            lockFuture.whenComplete((r, e) -> {
                synchronized (this) {
                    lockFuture = null;
                    version = r;
                }
            });
            return future;
        } else if (lockFuture == null) {
            return CompletableFuture.completedFuture(version);
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
