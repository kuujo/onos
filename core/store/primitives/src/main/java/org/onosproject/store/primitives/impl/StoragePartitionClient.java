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
package org.onosproject.store.primitives.impl;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Suppliers;
import io.atomix.catalyst.transport.Transport;
import io.atomix.copycat.client.CommunicationStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.metadata.CopycatSessionMetadata;
import org.onlab.util.HexString;
import org.onlab.util.OrderedExecutor;
import org.onosproject.store.primitives.DistributedPrimitiveCreator;
import org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMap;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMap;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentSetMultimap;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentTreeMap;
import org.onosproject.store.primitives.resources.impl.AtomixCounter;
import org.onosproject.store.primitives.resources.impl.AtomixDocumentTree;
import org.onosproject.store.primitives.resources.impl.AtomixIdGenerator;
import org.onosproject.store.primitives.resources.impl.AtomixLeaderElector;
import org.onosproject.store.primitives.resources.impl.AtomixWorkQueue;
import org.onosproject.store.serializers.KryoNamespaces;
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
import org.onosproject.store.service.PartitionClientInfo;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.WorkQueue;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * StoragePartition client.
 */
public class StoragePartitionClient implements DistributedPrimitiveCreator, Managed<StoragePartitionClient> {

    private final Logger log = getLogger(getClass());

    private final StoragePartition partition;
    private final Transport transport;
    private final io.atomix.catalyst.serializer.Serializer serializer;
    private final Executor sharedExecutor;
    private CopycatClient client;
    private static final String ATOMIC_VALUES_CONSISTENT_MAP_NAME = "onos-atomic-values";
    private final com.google.common.base.Supplier<AsyncConsistentMap<String, byte[]>> onosAtomicValuesMap =
            Suppliers.memoize(() -> newAsyncConsistentMap(ATOMIC_VALUES_CONSISTENT_MAP_NAME,
                                                          Serializer.using(KryoNamespaces.BASIC)));

    public StoragePartitionClient(StoragePartition partition,
            io.atomix.catalyst.serializer.Serializer serializer,
            Transport transport,
            Executor sharedExecutor) {
        this.partition = partition;
        this.serializer = serializer;
        this.transport = transport;
        this.sharedExecutor = sharedExecutor;
    }

    @Override
    public CompletableFuture<Void> open() {
        synchronized (StoragePartitionClient.this) {
            client = newCopycatClient(transport, serializer.clone());
        }
        return client.connect(partition.getMemberAddresses()).whenComplete((r, e) -> {
            if (e == null) {
                log.info("Successfully started client for partition {}", partition.getId());
            } else {
                log.info("Failed to start client for partition {}", partition.getId(), e);
            }
        }).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> close() {
        return client != null ? client.close() : CompletableFuture.completedFuture(null);
    }

    /**
     * Returns the executor provided by the given supplier or a serial executor if the supplier is {@code null}.
     *
     * @param executorSupplier the user-provided executor supplier
     * @return the executor
     */
    private Executor defaultExecutor(Supplier<Executor> executorSupplier) {
        return executorSupplier != null ? executorSupplier.get() : new OrderedExecutor(sharedExecutor);
    }

    @Override
    public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(
            String name, Serializer serializer, Supplier<Executor> executorSupplier) {
        AtomixConsistentMap atomixConsistentMap = new AtomixConsistentMap(client.sessionBuilder()
                .withName(name)
                .withType(DistributedPrimitive.Type.CONSISTENT_MAP.name())
                .withCommunicationStrategy(CommunicationStrategies.ANY)
                .build());

        AsyncConsistentMap<String, byte[]> rawMap =
                new DelegatingAsyncConsistentMap<String, byte[]>(atomixConsistentMap) {
                    @Override
                    public String name() {
                        return name;
                    }
                };

        // We have to ensure serialization is done on the Copycat threads since Kryo is not thread safe.
        AsyncConsistentMap<K, V> transcodedMap = DistributedPrimitives.newTranscodingMap(rawMap,
            key -> HexString.toHexString(serializer.encode(key)),
            string -> serializer.decode(HexString.fromHexString(string)),
            value -> value == null ? null : serializer.encode(value),
            bytes -> serializer.decode(bytes));

        return new ExecutingAsyncConsistentMap<>(transcodedMap, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public <V> AsyncConsistentTreeMap<V> newAsyncConsistentTreeMap(
            String name, Serializer serializer, Supplier<Executor> executorSupplier) {
        AtomixConsistentTreeMap atomixConsistentTreeMap = new AtomixConsistentTreeMap(client.sessionBuilder()
                .withName(name)
                .withType(DistributedPrimitive.Type.CONSISTENT_TREEMAP.name())
                .withCommunicationStrategy(CommunicationStrategies.ANY)
                .build());

        AsyncConsistentTreeMap<byte[]> rawMap =
                new DelegatingAsyncConsistentTreeMap<byte[]>(atomixConsistentTreeMap) {
                    @Override
                    public String name() {
                        return name;
                    }
                };

        AsyncConsistentTreeMap<V> transcodedMap =
                DistributedPrimitives.<V, byte[]>newTranscodingTreeMap(
                    rawMap,
                    value -> value == null ? null : serializer.encode(value),
                    bytes -> serializer.decode(bytes));

        return new ExecutingAsyncConsistentTreeMap<>(transcodedMap, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public <K, V> AsyncConsistentMultimap<K, V> newAsyncConsistentSetMultimap(
            String name, Serializer serializer, Supplier<Executor> executorSupplier) {
        AtomixConsistentSetMultimap atomixConsistentSetMultimap = new AtomixConsistentSetMultimap(client.sessionBuilder()
                .withName(name)
                .withType(DistributedPrimitive.Type.CONSISTENT_MULTIMAP.name())
                .withCommunicationStrategy(CommunicationStrategies.ANY)
                .build());

        AsyncConsistentMultimap<String, byte[]> rawMap =
                new DelegatingAsyncConsistentMultimap<String, byte[]>(
                        atomixConsistentSetMultimap) {
                    @Override
                    public String name() {
                        return super.name();
                    }
                };

        AsyncConsistentMultimap<K, V> transcodedMap =
                DistributedPrimitives.newTranscodingMultimap(
                        rawMap,
                        key -> HexString.toHexString(serializer.encode(key)),
                        string -> serializer.decode(HexString.fromHexString(string)),
                        value -> serializer.encode(value),
                        bytes -> serializer.decode(bytes));

        return new ExecutingAsyncConsistentMultimap<>(transcodedMap, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public <E> AsyncDistributedSet<E> newAsyncDistributedSet(
            String name, Serializer serializer, Supplier<Executor> executorSupplier) {
        return DistributedPrimitives.newSetFromMap(newAsyncConsistentMap(name, serializer, executorSupplier));
    }

    @Override
    public <K> AsyncAtomicCounterMap<K> newAsyncAtomicCounterMap(
            String name, Serializer serializer, Supplier<Executor> executorSupplier) {
        AtomixAtomicCounterMap atomixAtomicCounterMap = new AtomixAtomicCounterMap(client.sessionBuilder()
                .withName(name)
                .withType(DistributedPrimitive.Type.COUNTER_MAP.name())
                .withCommunicationStrategy(CommunicationStrategies.ANY)
                .build());

        AsyncAtomicCounterMap<K> transcodedMap =
                DistributedPrimitives.newTranscodingAtomicCounterMap(
                       atomixAtomicCounterMap,
                        key -> HexString.toHexString(serializer.encode(key)),
                        string -> serializer.decode(HexString.fromHexString(string)));

        return new ExecutingAsyncAtomicCounterMap<>(transcodedMap, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public AsyncAtomicCounter newAsyncCounter(String name, Supplier<Executor> executorSupplier) {
        AtomixCounter asyncCounter = new AtomixCounter(client.sessionBuilder()
                .withName(name)
                .withType(DistributedPrimitive.Type.COUNTER.name())
                .withCommunicationStrategy(CommunicationStrategies.LEADER)
                .build());
        return new ExecutingAsyncAtomicCounter(asyncCounter, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public AsyncAtomicIdGenerator newAsyncIdGenerator(String name, Supplier<Executor> executorSupplier) {
        AtomixCounter asyncCounter = new AtomixCounter(client.sessionBuilder()
                .withName(name)
                .withType(DistributedPrimitive.Type.COUNTER.name())
                .withCommunicationStrategy(CommunicationStrategies.LEADER)
                .build());
        AsyncAtomicIdGenerator asyncIdGenerator = new AtomixIdGenerator(asyncCounter);
        return new ExecutingAsyncAtomicIdGenerator(asyncIdGenerator, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public <V> AsyncAtomicValue<V> newAsyncAtomicValue(
            String name, Serializer serializer, Supplier<Executor> executorSupplier) {
       AsyncAtomicValue<V> asyncValue = new DefaultAsyncAtomicValue<>(name, serializer, onosAtomicValuesMap.get());
       return new ExecutingAsyncAtomicValue<>(asyncValue, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public <E> WorkQueue<E> newWorkQueue(String name, Serializer serializer, Supplier<Executor> executorSupplier) {
        AtomixWorkQueue atomixWorkQueue = new AtomixWorkQueue(client.sessionBuilder()
                .withName(name)
                .withType(DistributedPrimitive.Type.WORK_QUEUE.name())
                .withCommunicationStrategy(CommunicationStrategies.LEADER)
                .build());
        WorkQueue<E> workQueue = new DefaultDistributedWorkQueue<>(atomixWorkQueue, serializer);
        return new ExecutingWorkQueue<>(workQueue, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public <V> AsyncDocumentTree<V> newAsyncDocumentTree(
            String name, Serializer serializer, Supplier<Executor> executorSupplier) {
        AtomixDocumentTree atomixDocumentTree = new AtomixDocumentTree(client.sessionBuilder()
                .withName(name)
                .withType(DistributedPrimitive.Type.DOCUMENT_TREE.name())
                .withCommunicationStrategy(CommunicationStrategies.ANY)
                .build());
        AsyncDocumentTree<V> asyncDocumentTree = new DefaultDistributedDocumentTree<>(
                name, atomixDocumentTree, serializer);
        return new ExecutingAsyncDocumentTree<>(asyncDocumentTree, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public AsyncLeaderElector newAsyncLeaderElector(String name, Supplier<Executor> executorSupplier) {
        AtomixLeaderElector leaderElector = new AtomixLeaderElector(client.sessionBuilder()
                .withName(name)
                .withType(DistributedPrimitive.Type.LEADER_ELECTOR.name())
                .withCommunicationStrategy(CommunicationStrategies.LEADER)
                .build());
        leaderElector.setupCache().join();
        return new ExecutingAsyncLeaderElector(leaderElector, defaultExecutor(executorSupplier), sharedExecutor);
    }

    @Override
    public Set<String> getAsyncConsistentMapNames() {
        return client.metadata().getSessions(DistributedPrimitive.Type.CONSISTENT_MAP.name()).join()
                .stream()
                .map(CopycatSessionMetadata::name)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getAsyncAtomicCounterNames() {
        return client.metadata().getSessions(DistributedPrimitive.Type.COUNTER.name()).join()
                .stream()
                .map(CopycatSessionMetadata::name)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getWorkQueueNames() {
        return client.metadata().getSessions(DistributedPrimitive.Type.WORK_QUEUE.name()).join()
                .stream()
                .map(CopycatSessionMetadata::name)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean isOpen() {
        return client != null;
    }

    /**
     * Returns the {@link PartitionClientInfo information} for this client.
     * @return partition client information
     */
    public PartitionClientInfo clientInfo() {
        return new PartitionClientInfo(partition.getId(), partition.getMembers());
    }

    private CopycatClient newCopycatClient(Transport transport, io.atomix.catalyst.serializer.Serializer serializer) {
        CopycatClient client = CopycatClient.builder()
                .withTransport(transport)
                .withSerializer(serializer)
                .build();
        return new RetryingCopycatClient(new RecoveringCopycatClient(client), 5, 100);
    }
}
