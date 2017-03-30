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
package org.onosproject.store.primitives;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.onosproject.store.service.DistributedPrimitive;

/**
 * Distributed lock primitive.
 * <p>
 * The distributed lock provides a mechanism for nodes to synchronize access to cluster-wide shared resources.
 * This interface is an asynchronous version of Java's {@link java.util.concurrent.locks.Lock}.
 * <p>
 * Locks are implemented as a simple replicated queue that tracks which client currently holds a lock. When a lock
 * is {@link #lock() locked}, if the lock is available then the requesting instance will immediately receive
 * the lock, otherwise the lock request will be queued. When a lock is {@link #unlock() unlocked}, the next lock
 * requester will be granted the lock.
 * <p>
 * Distributed locks require no polling from the client. Locks are granted via session events published by the Atomix
 * cluster to the lock instance. In the event that a lock's client becomes disconnected from the cluster, its session
 * will expire after the configured cluster session timeout and the lock will be automatically released.
 * <h2>Detecting failures</h2>
 * Once a lock is acquired by a client, the cluster will monitor the lock holder's availability and release the lock
 * automatically if the client becomes disconnected from the cluster. However, in the event that a lock holder becomes
 * disconnected without crashing, it's possible for two processes to believe themselves to hold the lock simultaneously.
 * If the lock holder becomes disconnected the cluster may grant the lock to another process. For this reason it's
 * essential that clients monitor the {@link DistributedPrimitive.Status Status} of the lock. If the lock
 * transitions to the {@link org.onosproject.store.service.DistributedPrimitive.Status#SUSPENDED} state, that indicates
 * that the underlying client is unable to communicate with the cluster and another process may have been granted the
 * lock. Lock holders should monitor the lock for status changes and release the lock if the lock becomes inconsistent.
 * <p>
 * <pre>
 *   {@code
 *   lock.lock().thenRun(() -> {
 *     lock.onStateChange(state -> {
 *       if (state == DistributedLock.State.SUSPENDED) {
 *         lock.unlock();
 *         System.out.println("lost the lock");
 *       }
 *     });
 *     // Do stuff
 *   });
 *   }
 * </pre>
 */
public interface DistributedLock extends Lock, DistributedPrimitive {

    @Override
    default Type primitiveType() {
        return Type.LOCK;
    }

    /**
     * Attempts to acquire the lock if available within the given timeout.
     * <p>
     * <b>Timeouts and wall-clock time</b>
     * The provided {@code timeout} may not ultimately be representative of the actual timeout in the cluster. Because
     * of clock skew and determinism requirements, the actual timeout may be arbitrarily greater, but not less, than
     * the provided {@code timeout}. However, time will always progress monotonically. That is, time will never go
     * in reverse. A timeout of {@code 10} seconds will always be greater than a timeout of {@code 9} seconds, but the
     * times simply may not match actual wall-clock time.
     *
     * @param timeout the duration within which to acquire the lock
     * @return a completable future to be completed with a value indicating whether the lock was acquired
     */
    boolean tryLock(Duration timeout);

    /**
     * Attempts to acquire the lock if available within the given timeout.
     * <p>
     * <b>Timeouts and wall-clock time</b>
     * The provided {@code timeout} may not ultimately be representative of the actual timeout in the cluster. Because
     * of clock skew and determinism requirements, the actual timeout may be arbitrarily greater, but not less, than
     * the provided {@code timeout}. However, time will always progress monotonically. That is, time will never go
     * in reverse. A timeout of {@code 10} seconds will always be greater than a timeout of {@code 9} seconds, but the
     * times simply may not match actual wall-clock time.
     *
     * @param timeoutMillis the duration in milliseconds within which to acquire the lock
     * @return a completable future to be completed with a value indicating whether the lock was acquired
     */
    default boolean tryLock(long timeoutMillis) {
        return tryLock(Duration.ofMillis(timeoutMillis));
    }

    @Override
    default boolean tryLock(long timeout, TimeUnit unit) {
        return tryLock(Duration.ofMillis(unit.toMillis(timeout)));
    }

    @Override
    default void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    default Condition newCondition() {
        throw new UnsupportedOperationException();
    }

}
