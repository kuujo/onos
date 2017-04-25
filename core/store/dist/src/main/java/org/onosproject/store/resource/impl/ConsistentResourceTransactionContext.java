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
package org.onosproject.store.resource.impl;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.onlab.util.Tools;
import org.onosproject.net.resource.ContinuousResource;
import org.onosproject.net.resource.ContinuousResourceId;
import org.onosproject.net.resource.DiscreteResource;
import org.onosproject.net.resource.DiscreteResourceId;
import org.onosproject.net.resource.Resource;
import org.onosproject.net.resource.ResourceAllocation;
import org.onosproject.net.resource.ResourceCommitStatus;
import org.onosproject.net.resource.ResourceConsumer;
import org.onosproject.net.resource.ResourceConsumerId;
import org.onosproject.net.resource.ResourceId;
import org.onosproject.net.resource.ResourceTransactionContext;
import org.onosproject.net.resource.Resources;
import org.onosproject.store.service.CommitStatus;
import org.onosproject.store.service.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Consistent resource transaction context.
 */
public class ConsistentResourceTransactionContext
        extends ConsistentResourceStoreBase
        implements ResourceTransactionContext {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final TransactionalDiscreteResourceSubStore discreteSubstore;
    private final TransactionalContinuousResourceSubStore continuousSubstore;
    private final TransactionContext transactionContext;
    private ResourceCommitStatus commitStatus;

    ConsistentResourceTransactionContext(TransactionContext transactionContext) {
        this(new TransactionalDiscreteResourceSubStore(transactionContext),
                new TransactionalContinuousResourceSubStore(transactionContext),
                transactionContext);
    }

    private ConsistentResourceTransactionContext(
            TransactionalDiscreteResourceSubStore discreteSubstore,
            TransactionalContinuousResourceSubStore continuousSubstore,
            TransactionContext transactionContext) {
        super(discreteSubstore, continuousSubstore);
        this.discreteSubstore = discreteSubstore;
        this.continuousSubstore = continuousSubstore;
        this.transactionContext = transactionContext;
        transactionContext.begin();
    }

    @Override
    public void register(List<? extends Resource> resources) {
        // If the transaction was already aborted, skip this operation.
        if (!transactionContext.isOpen()) {
            return;
        }

        checkNotNull(resources);
        if (log.isTraceEnabled()) {
            resources.forEach(r -> log.trace("registering {}", r));
        }

        // the order is preserved by LinkedHashMap
        Map<DiscreteResource, List<Resource>> resourceMap = resources.stream()
                .filter(x -> x.parent().isPresent())
                .collect(Collectors.groupingBy(x -> x.parent().get(), LinkedHashMap::new, Collectors.toList()));

        for (Map.Entry<DiscreteResource, List<Resource>> entry : resourceMap.entrySet()) {
            DiscreteResourceId parentId = entry.getKey().id();
            if (!discreteSubstore.lookup(parentId).isPresent()) {
                abortTransaction(ResourceCommitStatus.FAILED_ALLOCATION);
                return;
            }

            if (!register(discreteSubstore, continuousSubstore, parentId, entry.getValue())) {
                abortTransaction(ResourceCommitStatus.FAILED_ALLOCATION);
                return;
            }
        }
    }

    @Override
    public void unregister(List<? extends ResourceId> ids) {
        // If the transaction was already aborted, skip this operation.
        if (!transactionContext.isOpen()) {
            return;
        }

        checkNotNull(ids);

        // Look up resources by resource IDs
        List<Resource> resources = ids.stream()
                .filter(x -> x.parent().isPresent())
                .map(x -> {
                    // avoid access to consistent map in the case of discrete resource
                    if (x instanceof DiscreteResourceId) {
                        return Optional.of(Resources.discrete((DiscreteResourceId) x).resource());
                    } else {
                        return continuousSubstore.lookup((ContinuousResourceId) x);
                    }
                })
                .flatMap(Tools::stream)
                .collect(Collectors.toList());
        // the order is preserved by LinkedHashMap
        Map<DiscreteResourceId, List<Resource>> resourceMap = resources.stream()
                .collect(Collectors.groupingBy(x -> x.parent().get().id(), LinkedHashMap::new, Collectors.toList()));

        for (Map.Entry<DiscreteResourceId, List<Resource>> entry : resourceMap.entrySet()) {
            if (!unregister(discreteSubstore, continuousSubstore, entry.getKey(), entry.getValue())) {
                log.warn("Failed to unregister {}: Failed to remove {} values.",
                        entry.getKey(), entry.getValue().size());
                log.debug("Failed to unregister {}: Failed to remove values: {}",
                        entry.getKey(), entry.getValue());
                abortTransaction(ResourceCommitStatus.FAILED_ALLOCATION);
                return;
            }
        }
    }

    @Override
    public void allocate(List<? extends Resource> resources, ResourceConsumer consumer) {
        // If the transaction was already aborted, skip this operation.
        if (!transactionContext.isOpen()) {
            return;
        }

        checkNotNull(resources);
        checkNotNull(consumer);

        for (Resource resource : resources) {
            if (resource instanceof DiscreteResource) {
                if (!discreteSubstore.allocate(consumer.consumerId(), (DiscreteResource) resource)) {
                    abortTransaction(ResourceCommitStatus.FAILED_ALLOCATION);
                    return;
                }
            } else if (resource instanceof ContinuousResource) {
                if (!continuousSubstore.allocate(consumer.consumerId(), (ContinuousResource) resource)) {
                    abortTransaction(ResourceCommitStatus.FAILED_ALLOCATION);
                    return;
                }
            }
        }
    }

    @Override
    public void release(List<ResourceAllocation> allocations) {
        // If the transaction was already aborted, skip this operation.
        if (!transactionContext.isOpen()) {
            return;
        }

        checkNotNull(allocations);

        for (ResourceAllocation allocation : allocations) {
            Resource resource = allocation.resource();
            ResourceConsumerId consumerId = allocation.consumerId();

            if (resource instanceof DiscreteResource) {
                if (!discreteSubstore.release(consumerId, (DiscreteResource) resource)) {
                    abortTransaction(ResourceCommitStatus.FAILED_ALLOCATION);
                    return;
                }
            } else if (resource instanceof ContinuousResource) {
                if (!continuousSubstore.release(consumerId, (ContinuousResource) resource)) {
                    abortTransaction(ResourceCommitStatus.FAILED_ALLOCATION);
                    return;
                }
            }
        }
    }

    /**
     * Appends the values to the existing values associated with the specified key.
     * If the map already has all the given values, appending will not happen.
     *
     * @param parent    resource ID of the parent under which the given resources are registered
     * @param resources resources to be registered
     * @return true if the operation succeeds, false otherwise.
     */
    // computational complexity: O(n) where n is the number of the specified value
    private boolean register(TransactionalDiscreteResourceSubStore discreteTxStore,
            TransactionalContinuousResourceSubStore continuousTxStore,
            DiscreteResourceId parent, List<Resource> resources) {
        // it's assumed that the passed "values" is non-empty

        // This is 2-pass scan. Nicer to have 1-pass scan
        Set<DiscreteResource> discreteResources = resources.stream()
                .filter(x -> x instanceof DiscreteResource)
                .map(x -> (DiscreteResource) x)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<ContinuousResource> continuousResources = resources.stream()
                .filter(x -> x instanceof ContinuousResource)
                .map(x -> (ContinuousResource) x)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return discreteTxStore.register(parent, discreteResources)
                && continuousTxStore.register(parent, continuousResources);
    }

    /**
     * Removes the values from the existing values associated with the specified key.
     * If the map doesn't contain the given values, removal will not happen.
     *
     * @param discreteTxStore   map holding multiple discrete resources for a key
     * @param continuousTxStore map holding multiple continuous resources for a key
     * @param parent            resource ID of the parent under which the given resources are unregistered
     * @param resources         resources to be unregistered
     * @return true if the operation succeeds, false otherwise
     */
    private boolean unregister(TransactionalDiscreteResourceSubStore discreteTxStore,
            TransactionalContinuousResourceSubStore continuousTxStore,
            DiscreteResourceId parent, List<Resource> resources) {
        // it's assumed that the passed "values" is non-empty

        // This is 2-pass scan. Nicer to have 1-pass scan
        Set<DiscreteResource> discreteResources = resources.stream()
                .filter(x -> x instanceof DiscreteResource)
                .map(x -> (DiscreteResource) x)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<ContinuousResource> continuousResources = resources.stream()
                .filter(x -> x instanceof ContinuousResource)
                .map(x -> (ContinuousResource) x)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return discreteTxStore.unregister(parent, discreteResources)
                && continuousTxStore.unregister(parent, continuousResources);
    }

    /**
     * Aborts the transaction with the given commit status.
     *
     * @param status the status with which to abort the transaction
     */
    private void abortTransaction(ResourceCommitStatus status) {
        transactionContext.abort();
        this.commitStatus = status;
    }

    @Override
    public ResourceCommitStatus commit() {
        // If the transaction is not open, that indicates it was already aborted. Return the set commit status.
        if (!transactionContext.isOpen()) {
            return commitStatus;
        }
        return transactionContext.commit().join() == CommitStatus.SUCCESS
                ? ResourceCommitStatus.SUCCEEDED
                : ResourceCommitStatus.FAILED_CONCURRENT_TRANSACTION;
    }

    @Override
    public void abort() {
        transactionContext.abort();
    }

    @Override
    public void close() {
        if (transactionContext.isOpen()) {
            transactionContext.abort();
        }
    }
}
