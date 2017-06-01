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

import java.util.Objects;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

/**
 * Atomix long state.
 */
public class AtomixCounterState extends StateMachine implements Snapshottable {
    private Long value = 0L;

    @Override
    protected void configure(StateMachineExecutor executor) {
        executor.register(AtomixCounterCommands.Set.class, this::set);
        executor.register(AtomixCounterCommands.Get.class, this::get);
        executor.register(AtomixCounterCommands.CompareAndSet.class, this::compareAndSet);
        executor.register(AtomixCounterCommands.IncrementAndGet.class, this::incrementAndGet);
        executor.register(AtomixCounterCommands.GetAndIncrement.class, this::getAndIncrement);
        executor.register(AtomixCounterCommands.AddAndGet.class, this::addAndGet);
        executor.register(AtomixCounterCommands.GetAndAdd.class, this::getAndAdd);
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeLong(value);
    }

    @Override
    public void install(SnapshotReader reader) {
        value = reader.readLong();
    }

    /**
     * Handles a set commit.
     */
    public void set(Commit<AtomixCounterCommands.Set> commit) {
        value = commit.operation().value();
    }

    /**
     * Handles a get commit.
     */
    public Long get(Commit<AtomixCounterCommands.Get> commit) {
        return value;
    }

    /**
     * Handles a compare and set commit.
     */
    public boolean compareAndSet(Commit<AtomixCounterCommands.CompareAndSet> commit) {
        if (Objects.equals(value, commit.operation().expect())) {
            value = commit.operation().update();
            return true;
        }
        return false;
    }

    /**
     * Handles an increment and get commit.
     */
    public long incrementAndGet(Commit<AtomixCounterCommands.IncrementAndGet> commit) {
        Long oldValue = value;
        value = oldValue + 1;
        return value;
    }

    /**
     * Handles a get and increment commit.
     */
    public long getAndIncrement(Commit<AtomixCounterCommands.GetAndIncrement> commit) {
        Long oldValue = value;
        value = oldValue + 1;
        return oldValue;
    }

    /**
     * Handles an add and get commit.
     */
    public long addAndGet(Commit<AtomixCounterCommands.AddAndGet> commit) {
        Long oldValue = value;
        value = oldValue + commit.operation().delta();
        return value;
    }

    /**
     * Handles a get and add commit.
     */
    public long getAndAdd(Commit<AtomixCounterCommands.GetAndAdd> commit) {
        Long oldValue = value;
        value = oldValue + commit.operation().delta();
        return oldValue;
    }
}