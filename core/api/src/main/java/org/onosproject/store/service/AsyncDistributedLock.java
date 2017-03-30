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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.onosproject.store.primitives.DefaultDistributedLock;
import org.onosproject.store.primitives.DistributedLock;

/**
 * Asynchronous distributed lock primitive.
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
public interface AsyncDistributedLock extends DistributedPrimitive {

    @Override
    default Type primitiveType() {
        return Type.LOCK;
    }

    /**
     * Acquires the lock.
     * <p>
     * When the lock is acquired, this lock instance will publish a lock request to the cluster and await
     * an event granting the lock to this instance. The returned {@link CompletableFuture} will not be completed
     * until the lock has been acquired.
     *
     * @return a completable future to be completed once the lock has been acquired
     */
    CompletableFuture<Long> lock();

    /**
     * Attempts to acquire the lock if available.
     * <p>
     * When the lock is acquired, this lock instance will publish an immediate lock request to the cluster. If the
     * lock is available, the lock will be granted and the returned {@link CompletableFuture} will be completed
     * successfully. If the lock is not immediately available, the {@link CompletableFuture} will be completed
     * with a {@code null} value.
     *
     * @return a completable future to be completed once the lock has been acquired
     */
    CompletableFuture<Long> tryLock();

    /**
     * Attempts to acquire the lock if available within the given timeout.
     * <p>
     * When the lock is acquired, this lock instance will publish an immediate lock request to the cluster. If the
     * lock is available, the lock will be granted and the returned {@link CompletableFuture} will be completed
     * successfully. If the lock is not immediately available, the lock request will be queued until the lock comes
     * available. If the lock {@code timeout} expires, the lock request will be cancelled and the returned
     * {@link CompletableFuture} will be completed successfully with a {@code null} result.
     * <p>
     * <b>Timeouts and wall-clock time</b>
     * The provided {@code timeout} may not ultimately be representative of the actual timeout in the cluster. Because
     * of clock skew and determinism requirements, the actual timeout may be arbitrarily greater, but not less, than
     * the provided {@code timeout}. However, time will always progress monotonically. That is, time will never go
     * in reverse. A timeout of {@code 10} seconds will always be greater than a timeout of {@code 9} seconds, but the
     * times simply may not match actual wall-clock time.
     *
     * @param timeout the duration within which to acquire the lock
     * @return a completable future to be completed once the lock has been acquired
     */
    CompletableFuture<Long> tryLock(Duration timeout);

    /**
     * Attempts to acquire the lock if available within the given timeout.
     * <p>
     * When the lock is acquired, this lock instance will publish an immediate lock request to the cluster. If the
     * lock is available, the lock will be granted and the returned {@link CompletableFuture} will be completed
     * successfully. If the lock is not immediately available, the lock request will be queued until the lock comes
     * available. If the lock {@code timeout} expires, the lock request will be cancelled and the returned
     * {@link CompletableFuture} will be completed successfully with a {@code null} result.
     * <p>
     * <b>Timeouts and wall-clock time</b>
     * The provided {@code timeout} may not ultimately be representative of the actual timeout in the cluster. Because
     * of clock skew and determinism requirements, the actual timeout may be arbitrarily greater, but not less, than
     * the provided {@code timeout}. However, time will always progress monotonically. That is, time will never go
     * in reverse. A timeout of {@code 10} seconds will always be greater than a timeout of {@code 9} seconds, but the
     * times simply may not match actual wall-clock time.
     *
     * @param timeoutMillis the duration in milliseconds within which to acquire the lock
     * @return a completable future to be completed once the lock has been acquired
     */
    default CompletableFuture<Long> tryLock(long timeoutMillis) {
        return tryLock(Duration.ofMillis(timeoutMillis));
    }

    /**
     * Attempts to acquire the lock if available within the given timeout.
     * <p>
     * When the lock is acquired, this lock instance will publish an immediate lock request to the cluster. If the
     * lock is available, the lock will be granted and the returned {@link CompletableFuture} will be completed
     * successfully. If the lock is not immediately available, the lock request will be queued until the lock comes
     * available. If the lock {@code timeout} expires, the lock request will be cancelled and the returned
     * {@link CompletableFuture} will be completed successfully with a {@code null} result.
     * <p>
     * <b>Timeouts and wall-clock time</b>
     * The provided {@code timeout} may not ultimately be representative of the actual timeout in the cluster. Because
     * of clock skew and determinism requirements, the actual timeout may be arbitrarily greater, but not less, than
     * the provided {@code timeout}. However, time will always progress monotonically. That is, time will never go
     * in reverse. A timeout of {@code 10} seconds will always be greater than a timeout of {@code 9} seconds, but the
     * times simply may not match actual wall-clock time.
     *
     * @param timeout the duration within which to acquire the lock
     * @param unit the unit of the given timeout
     * @return a completable future to be completed once the lock has been acquired
     */
    default CompletableFuture<Long> tryLock(long timeout, TimeUnit unit) {
        return tryLock(Duration.ofMillis(unit.toMillis(timeout)));
    }

    /**
     * Releases the lock.
     * <p>
     * When the lock is released, the lock instance will publish an unlock request to the cluster. Once the lock has
     * been released, if any other instances of this resource are waiting for a lock on this or another node, the lock
     * will be acquired by the waiting instance before this unlock operation is completed. Once the lock has been
     * released and granted to any waiters, the returned {@link CompletableFuture} will be completed.
     *
     * @return a completable future to be completed once the lock has been released
     */
    CompletableFuture<Void> unlock();

    /**
     * Returns a new {@link DistributedLock} that is backed by this instance.
     *
     * @param timeoutMillis timeout duration for the returned DistributedLock operations
     * @return new {@code DistributedLock} instance
     */
    default DistributedLock asDistributedLock(long timeoutMillis) {
        return new DefaultDistributedLock(this, timeoutMillis);
    }

    /**
     * Returns a new {@link DistributedLock} that is backed by this instance and with a default operation timeout.
     *
     * @return new {@code DistributedLock} instance
     */
    default DistributedLock asDistributedLock() {
        return asDistributedLock(DistributedPrimitive.DEFAULT_OPERTATION_TIMEOUT_MILLIS);
    }

}
