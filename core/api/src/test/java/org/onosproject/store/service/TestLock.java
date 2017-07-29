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
package org.onosproject.store.service;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test lock.
 */
public class TestLock implements AsyncDistributedLock {
    private final AtomicReference<Long> locked = new AtomicReference<>();
    private final AtomicLong lockId = new AtomicLong();
    private final Queue<CompletableFuture<Long>> locks = new ConcurrentLinkedQueue<>();

    @Override
    public String name() {
        return null;
    }

    @Override
    public CompletableFuture<Long> lock() {
        long lockId = this.lockId.incrementAndGet();
        if (locked.compareAndSet(null, lockId)) {
            return CompletableFuture.completedFuture(lockId);
        }

        CompletableFuture<Long> future = new CompletableFuture<>();
        locks.add(future);
        return future;
    }

    @Override
    public CompletableFuture<Long> tryLock() {
        long lockId = this.lockId.incrementAndGet();
        if (locked.compareAndSet(null, lockId)) {
            return CompletableFuture.completedFuture(lockId);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Long> tryLock(Duration timeout) {
        return tryLock();
    }

    @Override
    public CompletableFuture<Void> unlock() {
        Long lockId = locked.getAndSet(null);
        if (lockId != null) {
            return CompletableFuture.runAsync(() -> {
                CompletableFuture<Long> lock = locks.poll();
                if (lock != null) {
                    locked.set(this.lockId.incrementAndGet());
                    lock.complete(null);
                }
            });
        } else {
            throw new IllegalMonitorStateException();
        }
    }
}
