/*
 * Copyright 2016-present Open Networking Laboratory
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

import java.util.concurrent.CompletableFuture;

import io.atomix.copycat.client.session.CopycatSession;
import org.onosproject.store.service.AsyncAtomicCounter;

import static org.onosproject.store.primitives.resources.impl.AtomixCounterCommands.AddAndGet;
import static org.onosproject.store.primitives.resources.impl.AtomixCounterCommands.CompareAndSet;
import static org.onosproject.store.primitives.resources.impl.AtomixCounterCommands.Get;
import static org.onosproject.store.primitives.resources.impl.AtomixCounterCommands.GetAndAdd;
import static org.onosproject.store.primitives.resources.impl.AtomixCounterCommands.GetAndIncrement;
import static org.onosproject.store.primitives.resources.impl.AtomixCounterCommands.IncrementAndGet;
import static org.onosproject.store.primitives.resources.impl.AtomixCounterCommands.Set;

/**
 * Atomix counter implementation.
 */
public class AtomixCounter extends AbstractCopycatPrimitive implements AsyncAtomicCounter {

    public AtomixCounter(CopycatSession session) {
        super(session);
    }

    private long nullOrZero(Long value) {
        return value != null ? value : 0;
    }

    @Override
    public CompletableFuture<Long> get() {
        return session.submit(new Get()).thenApply(this::nullOrZero);
    }

    @Override
    public CompletableFuture<Void> set(long value) {
        return session.submit(new Set(value));
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(long expectedValue, long updateValue) {
        return session.submit(new CompareAndSet(expectedValue, updateValue));
    }

    @Override
    public CompletableFuture<Long> addAndGet(long delta) {
        return session.submit(new AddAndGet(delta));
    }

    @Override
    public CompletableFuture<Long> getAndAdd(long delta) {
        return session.submit(new GetAndAdd(delta));
    }

    @Override
    public CompletableFuture<Long> incrementAndGet() {
        return session.submit(new IncrementAndGet());
    }

    @Override
    public CompletableFuture<Long> getAndIncrement() {
        return session.submit(new GetAndIncrement());
    }
}