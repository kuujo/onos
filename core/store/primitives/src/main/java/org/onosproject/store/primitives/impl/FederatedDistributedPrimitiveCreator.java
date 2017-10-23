/*
 * Copyright 2016-present Open Networking Foundation
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import org.onlab.util.HexString;
import org.onosproject.cluster.PartitionId;
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
import org.onosproject.store.service.DistributedPrimitive;
import org.onosproject.store.service.DocumentPath;
import org.onosproject.store.service.Ordering;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.WorkQueue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@code DistributedPrimitiveCreator} that federates responsibility for creating
 * distributed primitives to a collection of other {@link DistributedPrimitiveCreator creators}.
 */
public class FederatedDistributedPrimitiveCreator implements DistributedPrimitiveCreator {
    private final DistributedPrimitiveCreator sharedPrimitiveCreator;
    private final TreeMap<PartitionId, DistributedPrimitiveCreator> isolatedPrimitiveCreators;
    private final List<PartitionId> sortedMemberPartitionIds;
    private final int buckets;

    public FederatedDistributedPrimitiveCreator(DistributedPrimitiveCreator sharedPrimitiveCreator, Map<PartitionId, DistributedPrimitiveCreator> isolatedPrimitiveCreators, int buckets) {
        this.sharedPrimitiveCreator = checkNotNull(sharedPrimitiveCreator);
        this.isolatedPrimitiveCreators = Maps.newTreeMap();
        this.isolatedPrimitiveCreators.putAll(checkNotNull(isolatedPrimitiveCreators));
        this.sortedMemberPartitionIds = Lists.newArrayList(isolatedPrimitiveCreators.keySet());
        this.buckets = buckets;
    }

    @Override
    public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(String name, Serializer serializer) {
        return newAsyncConsistentMap(name, serializer, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(String name, Serializer serializer, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        checkNotNull(serializer);
        switch (isolation) {
            case NONE:
                return newOpenAsyncConsistentMap(name, serializer);
            case LOCAL_VERSION:
                return newIsolatedAsyncConsistentMap(name, serializer);
            default:
                throw new AssertionError();
        }
    }

    private <K, V> AsyncConsistentMap<K, V> newOpenAsyncConsistentMap(String name, Serializer serializer) {
        return sharedPrimitiveCreator.newAsyncConsistentMap(name, serializer);
    }

    private <K, V> AsyncConsistentMap<K, V> newIsolatedAsyncConsistentMap(String name, Serializer serializer) {
        Map<PartitionId, AsyncConsistentMap<byte[], byte[]>> maps =
                Maps.transformValues(isolatedPrimitiveCreators,
                                     partition -> DistributedPrimitives.newTranscodingMap(
                                             partition.<String, byte[]>newAsyncConsistentMap(name, null),
                                             HexString::toHexString,
                                             HexString::fromHexString,
                                             Function.identity(),
                                             Function.identity()));
        Hasher<byte[]> hasher = key -> {
            int bucket = Math.abs(Hashing.murmur3_32().hashBytes(key).asInt()) % buckets;
            return sortedMemberPartitionIds.get(Hashing.consistentHash(bucket, sortedMemberPartitionIds.size()));
        };
        AsyncConsistentMap<byte[], byte[]> partitionedMap = new PartitionedAsyncConsistentMap<>(name, maps, hasher);
        return DistributedPrimitives.newTranscodingMap(partitionedMap,
                key -> serializer.encode(key),
                bytes -> serializer.decode(bytes),
                value -> value == null ? null : serializer.encode(value),
                bytes -> serializer.decode(bytes));
    }

    @Override
    public <V> AsyncConsistentTreeMap<V> newAsyncConsistentTreeMap(String name, Serializer serializer) {
        return null;
    }

    public <V> AsyncConsistentTreeMap<V> newAsyncConsistentTreeMap(String name, Serializer serializer, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        checkNotNull(serializer);
        switch (isolation) {
            case NONE:
                return newOpenAsyncConsistentTreeMap(name, serializer);
            case LOCAL_VERSION:
                return newIsolatedAsyncConsistentTreeMap(name, serializer);
            default:
                throw new AssertionError();
        }
    }

    private <V> AsyncConsistentTreeMap<V> newOpenAsyncConsistentTreeMap(String name, Serializer serializer) {
        return sharedPrimitiveCreator.newAsyncConsistentTreeMap(name, serializer);
    }

    private <V> AsyncConsistentTreeMap<V> newIsolatedAsyncConsistentTreeMap(String name, Serializer serializer) {
        return getCreator(name).newAsyncConsistentTreeMap(name, serializer);
    }

    @Override
    public <K, V> AsyncConsistentMultimap<K, V> newAsyncConsistentSetMultimap(String name, Serializer serializer) {
        return newAsyncConsistentSetMultimap(name, serializer, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public <K, V> AsyncConsistentMultimap<K, V> newAsyncConsistentSetMultimap(String name, Serializer serializer, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        checkNotNull(serializer);
        switch (isolation) {
            case NONE:
                return newOpenAsyncConsistentSetMultimap(name, serializer);
            case LOCAL_VERSION:
                return newIsolatedAsyncConsistentSetMultimap(name, serializer);
            default:
                throw new AssertionError();
        }
    }

    private <K, V> AsyncConsistentMultimap<K, V> newOpenAsyncConsistentSetMultimap(String name, Serializer serializer) {
        return sharedPrimitiveCreator.newAsyncConsistentSetMultimap(name, serializer);
    }

    private <K, V> AsyncConsistentMultimap<K, V> newIsolatedAsyncConsistentSetMultimap(String name, Serializer serializer) {
        return getCreator(name).newAsyncConsistentSetMultimap(name, serializer);
    }

    @Override
    public <E> AsyncDistributedSet<E> newAsyncDistributedSet(String name, Serializer serializer) {
        return newAsyncDistributedSet(name, serializer, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public <E> AsyncDistributedSet<E> newAsyncDistributedSet(String name, Serializer serializer, DistributedPrimitive.Isolation isolation) {
        return DistributedPrimitives.newSetFromMap(newAsyncConsistentMap(name, serializer, isolation));
    }

    @Override
    public <K> AsyncAtomicCounterMap<K> newAsyncAtomicCounterMap(String name, Serializer serializer) {
        return newAsyncAtomicCounterMap(name, serializer, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public <K> AsyncAtomicCounterMap<K> newAsyncAtomicCounterMap(String name, Serializer serializer, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        checkNotNull(serializer);
        switch (isolation) {
            case NONE:
                return newOpenAsyncAtomicCounterMap(name, serializer);
            case LOCAL_VERSION:
                return newIsolatedAsyncAtomicCounterMap(name, serializer);
            default:
                throw new AssertionError();
        }
    }

    private <K> AsyncAtomicCounterMap<K> newOpenAsyncAtomicCounterMap(String name, Serializer serializer) {
        return sharedPrimitiveCreator.newAsyncAtomicCounterMap(name, serializer);
    }

    private <K> AsyncAtomicCounterMap<K> newIsolatedAsyncAtomicCounterMap(String name, Serializer serializer) {
        return getCreator(name).newAsyncAtomicCounterMap(name, serializer);
    }

    @Override
    public AsyncAtomicCounter newAsyncCounter(String name) {
        return newAsyncCounter(name, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public AsyncAtomicCounter newAsyncCounter(String name, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        switch (isolation) {
            case NONE:
                return newOpenAsyncCounter(name);
            case LOCAL_VERSION:
                return newIsolatedAsyncCounter(name);
            default:
                throw new AssertionError();
        }
    }

    private AsyncAtomicCounter newOpenAsyncCounter(String name) {
        return sharedPrimitiveCreator.newAsyncCounter(name);
    }

    private AsyncAtomicCounter newIsolatedAsyncCounter(String name) {
        return getCreator(name).newAsyncCounter(name);
    }

    @Override
    public AsyncAtomicIdGenerator newAsyncIdGenerator(String name) {
        return newAsyncIdGenerator(name, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public AsyncAtomicIdGenerator newAsyncIdGenerator(String name, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        switch (isolation) {
            case NONE:
                return newOpenAsyncIdGenerator(name);
            case LOCAL_VERSION:
                return newIsolatedAsyncIdGenerator(name);
            default:
                throw new AssertionError();
        }
    }

    private AsyncAtomicIdGenerator newOpenAsyncIdGenerator(String name) {
        return sharedPrimitiveCreator.newAsyncIdGenerator(name);
    }

    private AsyncAtomicIdGenerator newIsolatedAsyncIdGenerator(String name) {
        return getCreator(name).newAsyncIdGenerator(name);
    }

    @Override
    public <V> AsyncAtomicValue<V> newAsyncAtomicValue(String name, Serializer serializer) {
        return newAsyncAtomicValue(name, serializer, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public <V> AsyncAtomicValue<V> newAsyncAtomicValue(String name, Serializer serializer, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        checkNotNull(serializer);
        switch (isolation) {
            case NONE:
                return newOpenAsyncAtomicValue(name, serializer);
            case LOCAL_VERSION:
                return newIsolatedAsyncAtomicValue(name, serializer);
            default:
                throw new AssertionError();
        }
    }

    private <V> AsyncAtomicValue<V> newOpenAsyncAtomicValue(String name, Serializer serializer) {
        return sharedPrimitiveCreator.newAsyncAtomicValue(name, serializer);
    }

    private <V> AsyncAtomicValue<V> newIsolatedAsyncAtomicValue(String name, Serializer serializer) {
        return getCreator(name).newAsyncAtomicValue(name, serializer);
    }

    @Override
    public AsyncLeaderElector newAsyncLeaderElector(String name, long electionTimeout, TimeUnit timeUnit) {
        return newAsyncLeaderElector(name, electionTimeout, timeUnit, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public AsyncLeaderElector newAsyncLeaderElector(String name, long electionTimeout, TimeUnit timeUnit, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        switch (isolation) {
            case NONE:
                return newOpenAsyncLeaderElector(name, electionTimeout, timeUnit);
            case LOCAL_VERSION:
                return newIsolatedAsyncLeaderElector(name, electionTimeout, timeUnit);
            default:
                throw new AssertionError();
        }
    }

    private AsyncLeaderElector newOpenAsyncLeaderElector(String name, long leaderTimeout, TimeUnit timeUnit) {
        return sharedPrimitiveCreator.newAsyncLeaderElector(name, leaderTimeout, timeUnit);
    }

    private AsyncLeaderElector newIsolatedAsyncLeaderElector(String name, long leaderTimeout, TimeUnit timeUnit) {
        Map<PartitionId, AsyncLeaderElector> leaderElectors =
                Maps.transformValues(isolatedPrimitiveCreators,
                        partition -> partition.newAsyncLeaderElector(name, leaderTimeout, timeUnit));
        Hasher<String> hasher = topic -> {
            int hashCode = Hashing.sha256().hashString(topic, Charsets.UTF_8).asInt();
            return sortedMemberPartitionIds.get(Math.abs(hashCode) % isolatedPrimitiveCreators.size());
        };
        return new PartitionedAsyncLeaderElector(name, leaderElectors, hasher);
    }

    @Override
    public <E> WorkQueue<E> newWorkQueue(String name, Serializer serializer) {
        return newWorkQueue(name, serializer, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public <E> WorkQueue<E> newWorkQueue(String name, Serializer serializer, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        checkNotNull(serializer);
        switch (isolation) {
            case NONE:
                return newOpenWorkQueue(name, serializer);
            case LOCAL_VERSION:
                return newIsolatedWorkQueue(name, serializer);
            default:
                throw new AssertionError();
        }
    }

    private <E> WorkQueue<E> newOpenWorkQueue(String name, Serializer serializer) {
        return sharedPrimitiveCreator.newWorkQueue(name, serializer);
    }

    private <E> WorkQueue<E> newIsolatedWorkQueue(String name, Serializer serializer) {
        return getCreator(name).newWorkQueue(name, serializer);
    }

    @Override
    public <V> AsyncDocumentTree<V> newAsyncDocumentTree(String name, Serializer serializer, Ordering ordering) {
        return newAsyncDocumentTree(name, serializer, ordering, DistributedPrimitive.Isolation.LOCAL_VERSION);
    }

    public <V> AsyncDocumentTree<V> newAsyncDocumentTree(String name, Serializer serializer, Ordering ordering, DistributedPrimitive.Isolation isolation) {
        checkNotNull(name);
        checkNotNull(serializer);
        checkNotNull(ordering);
        switch (isolation) {
            case NONE:
                return newOpenAsyncDocumentTree(name, serializer, ordering);
            case LOCAL_VERSION:
                return newIsolatedAsyncDocumentTree(name, serializer, ordering);
            default:
                throw new AssertionError();
        }
    }

    private <V> AsyncDocumentTree<V> newOpenAsyncDocumentTree(String name, Serializer serializer, Ordering ordering) {
        checkNotNull(name);
        checkNotNull(serializer);
        return sharedPrimitiveCreator.newAsyncDocumentTree(name, serializer, ordering);
    }

    private <V> AsyncDocumentTree<V> newIsolatedAsyncDocumentTree(String name, Serializer serializer, Ordering ordering) {
        checkNotNull(name);
        checkNotNull(serializer);
        Map<PartitionId, AsyncDocumentTree<V>> trees =
                Maps.transformValues(isolatedPrimitiveCreators, part -> part.<V>newAsyncDocumentTree(name, serializer, ordering));
        Hasher<DocumentPath> hasher = key -> {
            int bucket = Math.abs(Hashing.murmur3_32().hashUnencodedChars(String.valueOf(key)).asInt()) % buckets;
            return sortedMemberPartitionIds.get(Hashing.consistentHash(bucket, sortedMemberPartitionIds.size()));
        };
        return new PartitionedAsyncDocumentTree<>(name, trees, hasher);
    }

    @Override
    public Set<String> getAsyncConsistentMapNames() {
        // TODO: Include shared partition map names
        return isolatedPrimitiveCreators.values()
                      .stream()
                      .map(DistributedPrimitiveCreator::getAsyncConsistentMapNames)
                      .reduce(Sets::union)
                      .orElse(ImmutableSet.of());
    }

    @Override
    public Set<String> getAsyncAtomicCounterNames() {
        // TODO: Include shared partition counter names
        return isolatedPrimitiveCreators.values()
                      .stream()
                      .map(DistributedPrimitiveCreator::getAsyncAtomicCounterNames)
                      .reduce(Sets::union)
                      .orElse(ImmutableSet.of());
    }

    @Override
    public Set<String> getWorkQueueNames() {
        // TODO: Include shared partition queue names
        return isolatedPrimitiveCreators.values()
                      .stream()
                      .map(DistributedPrimitiveCreator::getWorkQueueNames)
                      .reduce(Sets::union)
                      .orElse(ImmutableSet.of());
    }

    /**
     * Returns the {@code DistributedPrimitiveCreator} to use for hosting a primitive.
     * @param name primitive name
     * @return primitive creator
     */
    private DistributedPrimitiveCreator getCreator(String name) {
        int hashCode = Hashing.sha256().hashString(name, Charsets.UTF_8).asInt();
        return isolatedPrimitiveCreators.get(sortedMemberPartitionIds.get(Math.abs(hashCode) % isolatedPrimitiveCreators.size()));
    }
}
