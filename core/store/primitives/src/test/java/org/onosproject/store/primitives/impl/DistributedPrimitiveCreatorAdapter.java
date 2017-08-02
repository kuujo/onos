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

import java.util.Set;

import org.onosproject.store.primitives.DistributedPrimitiveCreator;
import org.onosproject.store.service.AsyncAtomicCounter;
import org.onosproject.store.service.AsyncAtomicCounterMap;
import org.onosproject.store.service.AsyncAtomicIdGenerator;
import org.onosproject.store.service.AsyncAtomicValue;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.AsyncConsistentMultimap;
import org.onosproject.store.service.AsyncConsistentTreeMap;
import org.onosproject.store.service.AsyncDistributedSet;
import org.onosproject.store.service.AsyncDocumentTree;
import org.onosproject.store.service.AsyncLeaderElector;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.WorkQueue;

/**
 * Primitive creator adapter.
 */
public class DistributedPrimitiveCreatorAdapter implements DistributedPrimitiveCreator {
    @Override
    public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(String name, Serializer serializer) {
        return null;
    }

    @Override
    public <V> AsyncConsistentTreeMap<V> newAsyncConsistentTreeMap(String name, Serializer serializer) {
        return null;
    }

    @Override
    public <K, V> AsyncConsistentMultimap<K, V> newAsyncConsistentSetMultimap(String name, Serializer serializer) {
        return null;
    }

    @Override
    public <K> AsyncAtomicCounterMap<K> newAsyncAtomicCounterMap(String name, Serializer serializer) {
        return null;
    }

    @Override
    public AsyncAtomicCounter newAsyncCounter(String name) {
        return null;
    }

    @Override
    public AsyncAtomicIdGenerator newAsyncIdGenerator(String name) {
        return null;
    }

    @Override
    public <V> AsyncAtomicValue<V> newAsyncAtomicValue(String name, Serializer serializer) {
        return null;
    }

    @Override
    public <E> AsyncDistributedSet<E> newAsyncDistributedSet(String name, Serializer serializer) {
        return null;
    }

    @Override
    public AsyncLeaderElector newAsyncLeaderElector(String name) {
        return null;
    }

    @Override
    public <E> WorkQueue<E> newWorkQueue(String name, Serializer serializer) {
        return null;
    }

    @Override
    public <V> AsyncDocumentTree<V> newAsyncDocumentTree(String name, Serializer serializer) {
        return null;
    }

    @Override
    public Set<String> getAsyncConsistentMapNames() {
        return null;
    }

    @Override
    public Set<String> getAsyncAtomicCounterNames() {
        return null;
    }

    @Override
    public Set<String> getWorkQueueNames() {
        return null;
    }
}
