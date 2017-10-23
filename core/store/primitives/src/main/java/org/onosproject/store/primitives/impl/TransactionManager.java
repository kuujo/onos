/*
 * Copyright 2017-present Open Networking Foundation
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.ClusterEvent;
import org.onosproject.cluster.ClusterEventListener;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.NodeId;
import org.onosproject.cluster.PartitionId;
import org.onosproject.store.primitives.MapUpdate;
import org.onosproject.store.primitives.PartitionService;
import org.onosproject.store.primitives.TransactionId;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.ConsistentMap;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.StorageService;
import org.onosproject.store.service.Transaction;
import org.onosproject.store.service.TransactionAdminService;
import org.onosproject.store.service.TransactionException;
import org.onosproject.store.service.TransactionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction manager for managing state shared across multiple transactions.
 */
@Component
@Service
public class TransactionManager implements TransactionService, TransactionAdminService {
    private static final int DEFAULT_CACHE_SIZE = 100;
    private static final int DEFAULT_BUCKETS = 128;

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PartitionService partitionService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterService clusterService;

    private List<PartitionId> sortedPartitions;
    private AsyncConsistentMap<TransactionId, Transaction.State> transactions;
    private final LoadingCache<NodeId, ConsistentMap<TransactionId, Transaction.State>> nodeCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<NodeId, ConsistentMap<TransactionId, Transaction.State>>() {
                @Override
                public ConsistentMap<TransactionId, Transaction.State> load(NodeId key) throws Exception {
                    return storageService.<TransactionId, Transaction.State>consistentMapBuilder()
                            .withName("onos-transactions-" + key)
                            .withSerializer(Serializer.using(KryoNamespaces.API,
                                    Transaction.class,
                                    Transaction.State.class))
                            .build();
                }
            });

    private int partitionCacheSize = DEFAULT_CACHE_SIZE;
    private int numBuckets = DEFAULT_BUCKETS;
    private final Map<PartitionId, Cache<String, CachedMap>> partitionCache = Maps.newConcurrentMap();
    private final ClusterEventListener clusterListener = new InternalClusterEventListener();

    @Activate
    public void activate() {
        this.sortedPartitions = Lists.newArrayList(partitionService.getAllPartitionIds());
        Collections.sort(sortedPartitions);
        this.transactions = storageService.<TransactionId, Transaction.State>consistentMapBuilder()
                .withName("onos-transactions-" + clusterService.getLocalNode().id())
                .withSerializer(Serializer.using(KryoNamespaces.API,
                        Transaction.class,
                        Transaction.State.class))
                .buildAsyncMap();
        clusterService.addListener(clusterListener);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        clusterService.removeListener(clusterListener);
        log.info("Stopped");
    }

    @Override
    public Collection<TransactionId> getPendingTransactions() {
        return Futures.getUnchecked(transactions.keySet());
    }

    @Override
    public <K, V> PartitionedTransactionalMap<K, V> getTransactionalMap(
            String name,
            Serializer serializer,
            TransactionId transactionId) {
        Map<PartitionId, TransactionalMapParticipant<K, V>> partitions = new HashMap<>();
        for (PartitionId partitionId : partitionService.getAllPartitionIds()) {
            partitions.put(partitionId, getTransactionalMapPartition(
                    name, partitionId, serializer, transactionId));
        }

        Hasher<K> hasher = key -> {
            int bucket = Math.abs(Hashing.murmur3_32().hashBytes(serializer.encode(key)).asInt()) % numBuckets;
            int partition = Hashing.consistentHash(bucket, sortedPartitions.size());
            return sortedPartitions.get(partition);
        };
        return new PartitionedTransactionalMap<>(partitions, hasher);
    }

    @SuppressWarnings("unchecked")
    private <K, V> TransactionalMapParticipant<K, V> getTransactionalMapPartition(
            String mapName,
            PartitionId partitionId,
            Serializer serializer,
            TransactionId transactionId) {
        Cache<String, CachedMap> mapCache = partitionCache.computeIfAbsent(partitionId, p ->
                CacheBuilder.newBuilder().maximumSize(partitionCacheSize / partitionService.getNumberOfPartitions()).build());
        try {
            CachedMap<K, V> cachedMap = mapCache.get(mapName,
                    () -> new CachedMap<>(partitionService.getDistributedPrimitiveCreator(partitionId)
                            .newAsyncConsistentMap(mapName, serializer)));

            Transaction<MapUpdate<K, V>> transaction = new Transaction<>(
                    transactionId,
                    cachedMap.baseMap);
            return new DefaultTransactionalMapParticipant<>(cachedMap.cachedMap.asConsistentMap(), transaction);
        } catch (ExecutionException e) {
            throw new TransactionException(e);
        }
    }

    @Override
    public TransactionId createTransaction() {
        return TransactionId.from(UUID.randomUUID().toString());
    }

    @Override
    public CompletableFuture<Void> updateTransaction(TransactionId transactionId, Transaction.State state) {
        return transactions.put(transactionId, state).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> removeTransaction(TransactionId transactionId) {
        return transactions.remove(transactionId).thenApply(v -> null);
    }

    private ConsistentMap<TransactionId, Transaction.State> getStateMap(NodeId nodeId) {
        return nodeCache.getUnchecked(nodeId);
    }

    /**
     * Called when a controller node crashes.
     *
     * @param nodeId the node ID of the node that crashed
     */
    private void handleNodeFailed(NodeId nodeId) {
        // Get the transactional map for the node that crashed.
        ConsistentMap<TransactionId, Transaction.State> nodeTransactions = nodeCache.getUnchecked(nodeId);

        // Commit or roll back transactions
    }

    private static class CachedMap<K, V> {
        private final AsyncConsistentMap<K, V> baseMap;
        private final AsyncConsistentMap<K, V> cachedMap;

        public CachedMap(AsyncConsistentMap<K, V> baseMap) {
            this.baseMap = baseMap;
            this.cachedMap = DistributedPrimitives.newCachingMap(baseMap);
        }
    }

    private class InternalClusterEventListener implements ClusterEventListener {
        @Override
        public void event(ClusterEvent event) {
            if (event.type() == ClusterEvent.Type.INSTANCE_DEACTIVATED) {
                handleNodeFailed(event.subject().id());
            }
        }
    }
}