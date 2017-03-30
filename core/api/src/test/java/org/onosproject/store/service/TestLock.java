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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test lock.
 */
public class TestLock implements AsyncDistributedLock {
    private final AtomicBoolean locked = new AtomicBoolean();
    private final Queue<CompletableFuture<Void>> locks = new ConcurrentLinkedQueue<>();

    @Override
    public String name() {
        return null;
    }

    @Override
    public CompletableFuture<Void> lock() {
        if (locked.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        locks.add(future);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> tryLock() {
        if (locked.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(true);
        }
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> tryLock(Duration timeout) {
        return tryLock();
    }

    @Override
    public CompletableFuture<Void> unlock() {
        if (locked.compareAndSet(true, false)) {
            return CompletableFuture.runAsync(() -> {
                CompletableFuture<Void> lock = locks.poll();
                if (lock != null) {
                    locked.set(true);
                    lock.complete(null);
                }
            });
        } else {
            throw new IllegalMonitorStateException();
        }
    }
}
