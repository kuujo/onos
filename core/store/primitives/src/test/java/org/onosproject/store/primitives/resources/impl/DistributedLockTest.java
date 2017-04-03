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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.atomix.Atomix;
import io.atomix.concurrent.DistributedLock;
import io.atomix.resource.ResourceType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.onosproject.store.primitives.impl.ReentrantDistributedLock;
import org.onosproject.store.service.AsyncDistributedLock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Basic distributed lock test.
 */
public class DistributedLockTest extends AtomixTestBase {

    @BeforeClass
    public static void preTestSetup() throws Throwable {
        createCopycatServers(3);
    }

    @AfterClass
    public static void postTestCleanup() throws Exception {
        clearTests();
    }

    @Override
    protected ResourceType resourceType() {
        return new ResourceType(DistributedLock.class);
    }

    @Test
    public void testLock() throws Throwable {
        Atomix client1 = createAtomixClient();
        Atomix client2 = createAtomixClient();

        AsyncDistributedLock lock1 = client1.getLock("test-lock-1")
                .thenApply(l -> new AtomixDistributedLock("test-lock-1", l))
                .join();
        AsyncDistributedLock lock2 = client2.getLock("test-lock-1")
                .thenApply(l -> new AtomixDistributedLock("test-lock-1", l))
                .join();

        lock1.lock().join();

        AtomicBoolean locked1 = new AtomicBoolean();
        AtomicBoolean locked2 = new AtomicBoolean();

        CompletableFuture<Void> lock1Future = lock1.lock().thenRun(() -> locked1.set(true));
        assertFalse(locked1.get());

        CompletableFuture<Void> lock2Future = lock2.lock().thenRun(() -> locked2.set(true));
        assertFalse(locked2.get());

        lock1.unlock().join();
        lock1Future.join();
        assertTrue(locked1.get());
        assertFalse(locked2.get());

        lock1.unlock().join();
        lock2Future.join();
        assertTrue(locked2.get());
    }

    @Test
    public void testTryLock() throws Throwable {
        Atomix client1 = createAtomixClient();
        Atomix client2 = createAtomixClient();

        AsyncDistributedLock lock1 = client1.getLock("test-try-lock-1")
                .thenApply(l -> new AtomixDistributedLock("test-try-lock-1", l))
                .join();
        AsyncDistributedLock lock2 = client2.getLock("test-try-lock-1")
                .thenApply(l -> new AtomixDistributedLock("test-try-lock-1", l))
                .join();

        assertTrue(lock1.tryLock().join());

        AtomicBoolean locked1 = new AtomicBoolean();
        AtomicBoolean locked2 = new AtomicBoolean();

        assertFalse(lock1.tryLock().join());

        CompletableFuture<Void> lock1Future = lock1.tryLock(Duration.ofSeconds(5)).thenAccept(locked -> {
            assertTrue(locked);
            locked1.set(true);
        });
        assertFalse(locked1.get());

        CompletableFuture<Void> lock2Future = lock2.tryLock(Duration.ofSeconds(5)).thenAccept(locked -> {
            assertTrue(locked);
            locked2.set(true);
        });
        assertFalse(locked2.get());

        lock1.unlock().join();
        lock1Future.join();
        assertTrue(locked1.get());
        assertFalse(locked2.get());

        lock1.unlock().join();
        lock2Future.join();
        assertTrue(locked2.get());
    }

    @Test
    public void testReentrantLock() throws Throwable {
        Atomix client1 = createAtomixClient();
        Atomix client2 = createAtomixClient();

        AsyncDistributedLock lock1 = client1.getLock("test-reentrant-lock-1")
                .thenApply(l -> new ReentrantDistributedLock(new AtomixDistributedLock("test-reentrant-lock-1", l)))
                .join();
        AsyncDistributedLock lock2 = client2.getLock("test-reentrant-lock-1")
                .thenApply(l -> new ReentrantDistributedLock(new AtomixDistributedLock("test-reentrant-lock-1", l)))
                .join();

        lock1.lock().join();
        lock1.lock().join();

        AtomicBoolean locked = new AtomicBoolean();
        CompletableFuture<Void> lockFuture = lock2.lock().thenRun(() -> locked.set(true));
        assertFalse(locked.get());

        lock1.unlock().join();
        assertFalse(locked.get());

        CompletableFuture<Void> unlockFuture = lock1.unlock();
        assertFalse(locked.get());
        unlockFuture.join();

        lockFuture.join();
        assertTrue(locked.get());
    }

    @Test
    public void testReentrantTryLock() throws Throwable {
        Atomix client1 = createAtomixClient();
        Atomix client2 = createAtomixClient();

        AsyncDistributedLock lock1 = client1.getLock("test-reentrant-try-lock-1")
                .thenApply(l -> new ReentrantDistributedLock(new AtomixDistributedLock("test-reentrant-try-lock-1", l)))
                .join();
        AsyncDistributedLock lock2 = client2.getLock("test-reentrant-try-lock-1")
                .thenApply(l -> new ReentrantDistributedLock(new AtomixDistributedLock("test-reentrant-try-lock-1", l)))
                .join();

        assertTrue(lock1.tryLock().join());
        assertTrue(lock1.tryLock().join());

        assertFalse(lock2.tryLock().join());

        AtomicBoolean locked = new AtomicBoolean();
        CompletableFuture<Void> lockFuture = lock2.tryLock(Duration.ofSeconds(5)).thenRun(() -> locked.set(true));
        assertFalse(locked.get());

        lock1.unlock().join();
        assertFalse(locked.get());

        CompletableFuture<Void> unlockFuture = lock1.unlock();
        assertFalse(locked.get());
        unlockFuture.join();

        lockFuture.join();
        assertTrue(locked.get());
    }

}
