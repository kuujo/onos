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
package org.onosproject.store.resource.impl;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.onosproject.net.resource.DiscreteResource;
import org.onosproject.net.resource.DiscreteResourceId;
import org.onosproject.net.resource.Resource;
import org.onosproject.net.resource.ResourceAllocation;
import org.onosproject.net.resource.ResourceConsumerId;
import org.onosproject.net.resource.Resources;
import org.onosproject.store.service.TransactionContext;
import org.onosproject.store.service.TransactionalMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.onosproject.store.resource.impl.ConsistentResourceStore.SERIALIZER;

/**
 * Transactional substore for discrete resources.
 */
class TransactionalDiscreteResourceSubStore implements
        DiscreteResourceSubStore, TransactionalResourceSubStore<DiscreteResourceId, DiscreteResource> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final TransactionalMap<DiscreteResourceId, DiscreteResources> childMap;
    private final TransactionalMap<DiscreteResourceId, ResourceConsumerId> consumers;

    TransactionalDiscreteResourceSubStore(TransactionContext transactionContext) {
        this.childMap = transactionContext.getTransactionalMap(MapNames.DISCRETE_CHILD_MAP, SERIALIZER);
        this.consumers = transactionContext.getTransactionalMap(MapNames.DISCRETE_CONSUMER_MAP, SERIALIZER);
    }

    // check the existence in the set: O(1) operation
    @Override
    public Optional<DiscreteResource> lookup(DiscreteResourceId id) {
        if (!id.parent().isPresent()) {
            return Optional.of(Resource.ROOT);
        }

        DiscreteResources values = childMap.get(id.parent().get());
        if (values == null) {
            return Optional.empty();
        }

        return values.lookup(id);
    }

    @Override
    public boolean register(DiscreteResourceId parent, Set<DiscreteResource> resources) {
        // short-circuit: receiving empty resource is regarded as success
        if (resources.isEmpty()) {
            return true;
        }

        DiscreteResources requested = DiscreteResources.of(resources);
        DiscreteResources oldValues = childMap.putIfAbsent(parent, requested);
        if (oldValues == null) {
            return true;
        }

        DiscreteResources addedValues = requested.difference(oldValues);
        // no new value, then no-op
        if (addedValues.isEmpty()) {
            // don't write to map because all values are already stored
            return true;
        }

        DiscreteResources newValues = oldValues.add(addedValues);
        return childMap.replace(parent, oldValues, newValues);
    }

    @Override
    public boolean unregister(DiscreteResourceId parent, Set<DiscreteResource> resources) {
        // short-circuit: receiving empty resource is regarded as success
        if (resources.isEmpty()) {
            return true;
        }

        // even if one of the resources is allocated to a consumer,
        // all unregistrations are regarded as failure
        boolean allocated = resources.stream().anyMatch(x -> isAllocated(x.id()));
        if (allocated) {
            log.warn("Failed to unregister {}: allocation exists", parent);
            return false;
        }

        DiscreteResources oldValues = childMap.putIfAbsent(parent, DiscreteResources.empty());
        if (oldValues == null) {
            log.trace("No-Op removing values. key {} did not exist", parent);
            return true;
        }

        if (!oldValues.containsAny(resources)) {
            // don't write map because none of the values are stored
            log.trace("No-Op removing values. key {} did not contain {}", parent, resources);
            return true;
        }

        DiscreteResources requested = DiscreteResources.of(resources);
        DiscreteResources newValues = oldValues.difference(requested);
        return childMap.replace(parent, oldValues, newValues);
    }

    @Override
    public boolean isAllocated(DiscreteResourceId id) {
        return consumers.get(id) != null;
    }

    @Override
    public boolean allocate(ResourceConsumerId consumerId, DiscreteResource resource) {
        // if the resource is not registered, then abort
        Optional<DiscreteResource> lookedUp = lookup(resource.id());
        if (!lookedUp.isPresent()) {
            return false;
        }

        ResourceConsumerId oldValue = consumers.put(resource.id(), consumerId);
        return oldValue == null;
    }

    @Override
    public boolean release(ResourceConsumerId consumerId, DiscreteResource resource) {
        // if this single release fails (because the resource is allocated to another consumer)
        // the whole release fails
        return consumers.remove(resource.id(), consumerId);
    }

    @Override
    public List<ResourceAllocation> getResourceAllocations(DiscreteResourceId resourceId) {
        ResourceConsumerId consumerId = consumers.get(resourceId);
        if (consumerId == null) {
            return ImmutableList.of();
        }

        return ImmutableList.of(new ResourceAllocation(Resources.discrete(resourceId).resource(), consumerId));
    }

    @Override
    public Set<DiscreteResource> getChildResources(DiscreteResourceId resourceId) {
        DiscreteResources children = childMap.get(resourceId);

        if (children == null) {
            return ImmutableSet.of();
        }

        return children.values();
    }

    @Override
    public Set<DiscreteResource> getChildResources(DiscreteResourceId resourceId, Class<?> type) {
        DiscreteResources children = childMap.get(resourceId);

        if (children == null) {
            return ImmutableSet.of();
        }

        return children.valuesOf(type);
    }

    @Override
    public boolean isAvailable(DiscreteResource resource) {
        return getResourceAllocations(resource.id()).isEmpty();
    }

    @Override
    public Stream<DiscreteResource> getAllocatedResources(DiscreteResourceId parent, Class<?> type) {
        Set<DiscreteResource> children = getChildResources(parent);
        if (children.isEmpty()) {
            return Stream.of();
        }

        return children.stream()
                .filter(x -> x.isTypeOf(type))
                .filter(x -> consumers.containsKey(x.id()));
    }

    @Override
    public Stream<DiscreteResource> getResources(ResourceConsumerId consumerId) {
        throw new UnsupportedOperationException();
    }
}
