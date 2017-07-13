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

import java.util.HashMap;
import java.util.Map;

import io.atomix.protocols.raft.service.AbstractRaftService;
import io.atomix.protocols.raft.service.Commit;
import io.atomix.protocols.raft.service.RaftServiceExecutor;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import org.onlab.util.KryoNamespace;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.AddAndGet;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.DecrementAndGet;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.Get;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.GetAndAdd;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.GetAndDecrement;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.GetAndIncrement;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.IncrementAndGet;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.Put;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.PutIfAbsent;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.Remove;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.RemoveValue;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.Replace;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.Serializer;

import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.ADD_AND_GET;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.CLEAR;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.DECREMENT_AND_GET;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.GET;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.GET_AND_ADD;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.GET_AND_DECREMENT;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.GET_AND_INCREMENT;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.INCREMENT_AND_GET;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.IS_EMPTY;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.PUT;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.PUT_IF_ABSENT;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.REMOVE;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.REMOVE_VALUE;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.REPLACE;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.SIZE;

/**
 * Atomic counter map state for Atomix.
 * <p>
 * The counter map state is implemented as a snapshottable state machine. Snapshots are necessary
 * since incremental compaction is impractical for counters where the value of a counter is the sum
 * of all its increments. Note that this snapshotting large state machines may risk blocking of the
 * Raft cluster with the current implementation of snapshotting in Copycat.
 */
public class AtomixAtomicCounterMapService extends AbstractRaftService {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
            .register(KryoNamespaces.BASIC)
            .register(AtomixAtomicCounterMapOperations.NAMESPACE)
            .build());

    private Map<String, Long> map = new HashMap<>();

    @Override
    protected void configure(RaftServiceExecutor executor) {
        executor.register(PUT, SERIALIZER::decode, this::put, SERIALIZER::encode);
        executor.register(PUT_IF_ABSENT, SERIALIZER::decode, this::putIfAbsent, SERIALIZER::encode);
        executor.register(GET, SERIALIZER::decode, this::get, SERIALIZER::encode);
        executor.register(REPLACE, SERIALIZER::decode, this::replace, SERIALIZER::encode);
        executor.register(REMOVE, SERIALIZER::decode, this::remove, SERIALIZER::encode);
        executor.register(REMOVE_VALUE, SERIALIZER::decode, this::removeValue, SERIALIZER::encode);
        executor.register(GET_AND_INCREMENT, SERIALIZER::decode, this::getAndIncrement, SERIALIZER::encode);
        executor.register(GET_AND_DECREMENT, SERIALIZER::decode, this::getAndDecrement, SERIALIZER::encode);
        executor.register(INCREMENT_AND_GET, SERIALIZER::decode, this::incrementAndGet, SERIALIZER::encode);
        executor.register(DECREMENT_AND_GET, SERIALIZER::decode, this::decrementAndGet, SERIALIZER::encode);
        executor.register(ADD_AND_GET, SERIALIZER::decode, this::addAndGet, SERIALIZER::encode);
        executor.register(GET_AND_ADD, SERIALIZER::decode, this::getAndAdd, SERIALIZER::encode);
        executor.register(SIZE, this::size, SERIALIZER::encode);
        executor.register(IS_EMPTY, this::isEmpty, SERIALIZER::encode);
        executor.register(CLEAR, this::clear);
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeObject(map, SERIALIZER::encode);
    }

    @Override
    public void install(SnapshotReader reader) {
        map = reader.readObject(SERIALIZER::decode);
    }

    /**
     * Returns the primitive value for the given primitive wrapper.
     */
    private long primitive(Long value) {
        if (value != null) {
            return value;
        } else {
            return 0;
        }
    }

    /**
     * Handles a {@link Put} command which implements {@link AtomixAtomicCounterMap#put(String, long)}.
     *
     * @param commit put commit
     * @return put result
     */
    protected long put(Commit<Put> commit) {
        return primitive(map.put(commit.value().key(), commit.value().value()));
    }

    /**
     * Handles a {@link PutIfAbsent} command which implements {@link AtomixAtomicCounterMap#putIfAbsent(String, long)}.
     *
     * @param commit putIfAbsent commit
     * @return putIfAbsent result
     */
    protected long putIfAbsent(Commit<PutIfAbsent> commit) {
        return primitive(map.putIfAbsent(commit.value().key(), commit.value().value()));
    }

    /**
     * Handles a {@link Get} query which implements {@link AtomixAtomicCounterMap#get(String)}}.
     *
     * @param commit get commit
     * @return get result
     */
    protected long get(Commit<Get> commit) {
        return primitive(map.get(commit.value().key()));
    }

    /**
     * Handles a {@link Replace} command which implements {@link AtomixAtomicCounterMap#replace(String, long, long)}.
     *
     * @param commit replace commit
     * @return replace result
     */
    protected boolean replace(Commit<Replace> commit) {
        Long value = map.get(commit.value().key());
        if (value == null) {
            if (commit.value().replace() == 0) {
                map.put(commit.value().key(), commit.value().value());
                return true;
            } else {
                return false;
            }
        } else if (value == commit.value().replace()) {
            map.put(commit.value().key(), commit.value().value());
            return true;
        }
        return false;
    }

    /**
     * Handles a {@link Remove} command which implements {@link AtomixAtomicCounterMap#remove(String)}.
     *
     * @param commit remove commit
     * @return remove result
     */
    protected long remove(Commit<Remove> commit) {
        return primitive(map.remove(commit.value().key()));
    }

    /**
     * Handles a {@link RemoveValue} command which implements {@link AtomixAtomicCounterMap#remove(String, long)}.
     *
     * @param commit removeValue commit
     * @return removeValue result
     */
    protected boolean removeValue(Commit<RemoveValue> commit) {
        Long value = map.get(commit.value().key());
        if (value == null) {
            if (commit.value().value() == 0) {
                map.remove(commit.value().key());
                return true;
            }
            return false;
        } else if (value == commit.value().value()) {
            map.remove(commit.value().key());
            return true;
        }
        return false;
    }

    /**
     * Handles a {@link GetAndIncrement} command which implements
     * {@link AtomixAtomicCounterMap#getAndIncrement(String)}.
     *
     * @param commit getAndIncrement commit
     * @return getAndIncrement result
     */
    protected long getAndIncrement(Commit<GetAndIncrement> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), value + 1);
        return value;
    }

    /**
     * Handles a {@link GetAndDecrement} command which implements
     * {@link AtomixAtomicCounterMap#getAndDecrement(String)}.
     *
     * @param commit getAndDecrement commit
     * @return getAndDecrement result
     */
    protected long getAndDecrement(Commit<GetAndDecrement> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), value - 1);
        return value;
    }

    /**
     * Handles a {@link IncrementAndGet} command which implements
     * {@link AtomixAtomicCounterMap#incrementAndGet(String)}.
     *
     * @param commit incrementAndGet commit
     * @return incrementAndGet result
     */
    protected long incrementAndGet(Commit<IncrementAndGet> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), ++value);
        return value;
    }

    /**
     * Handles a {@link DecrementAndGet} command which implements
     * {@link AtomixAtomicCounterMap#decrementAndGet(String)}.
     *
     * @param commit decrementAndGet commit
     * @return decrementAndGet result
     */
    protected long decrementAndGet(Commit<DecrementAndGet> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), --value);
        return value;
    }

    /**
     * Handles a {@link AddAndGet} command which implements {@link AtomixAtomicCounterMap#addAndGet(String, long)}.
     *
     * @param commit addAndGet commit
     * @return addAndGet result
     */
    protected long addAndGet(Commit<AddAndGet> commit) {
        long value = primitive(map.get(commit.value().key()));
        value += commit.value().delta();
        map.put(commit.value().key(), value);
        return value;
    }

    /**
     * Handles a {@link GetAndAdd} command which implements {@link AtomixAtomicCounterMap#getAndAdd(String, long)}.
     *
     * @param commit getAndAdd commit
     * @return getAndAdd result
     */
    protected long getAndAdd(Commit<GetAndAdd> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), value + commit.value().delta());
        return value;
    }

    /**
     * Handles a {@code Size} query which implements {@link AtomixAtomicCounterMap#size()}.
     *
     * @param commit size commit
     * @return size result
     */
    protected int size(Commit<Void> commit) {
        return map.size();
    }

    /**
     * Handles an {@code IsEmpty} query which implements {@link AtomixAtomicCounterMap#isEmpty()}.
     *
     * @param commit isEmpty commit
     * @return isEmpty result
     */
    protected boolean isEmpty(Commit<Void> commit) {
        return map.isEmpty();
    }

    /**
     * Handles a {@code Clear} command which implements {@link AtomixAtomicCounterMap#clear()}.
     *
     * @param commit clear commit
     */
    protected void clear(Commit<Void> commit) {
        map.clear();
    }
}