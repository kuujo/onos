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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import org.onosproject.net.resource.ContinuousResource;
import org.onosproject.net.resource.ContinuousResourceId;
import org.onosproject.net.resource.DiscreteResource;
import org.onosproject.net.resource.DiscreteResourceId;
import org.onosproject.net.resource.Resource;
import org.onosproject.net.resource.ResourceAllocation;
import org.onosproject.net.resource.ResourceConsumer;
import org.onosproject.net.resource.ResourceId;
import org.onosproject.net.resource.ResourceStoreBase;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Consistent resource store base.
 */
public class ConsistentResourceStoreBase implements ResourceStoreBase {
    private final DiscreteResourceSubStore discreteStore;
    private final ContinuousResourceSubStore continuousStore;

    public ConsistentResourceStoreBase(
            DiscreteResourceSubStore discreteStore,
            ContinuousResourceSubStore continuousStore) {
        this.discreteStore = discreteStore;
        this.continuousStore = continuousStore;
    }

    // Computational complexity: O(1) if the resource is discrete type.
    // O(n) if the resource is continuous type where n is the number of the existing allocations for the resource
    @Override
    public List<ResourceAllocation> getResourceAllocations(ResourceId id) {
        checkNotNull(id);
        checkArgument(id instanceof DiscreteResourceId || id instanceof ContinuousResourceId);

        if (id instanceof DiscreteResourceId) {
            return discreteStore.getResourceAllocations((DiscreteResourceId) id);
        } else {
            return continuousStore.getResourceAllocations((ContinuousResourceId) id);
        }
    }

    // computational complexity: O(1) if the resource is discrete type.
    // O(n) if the resource is continuous type where n is the number of the children of
    // the specified resource's parent
    @Override
    public boolean isAvailable(Resource resource) {
        checkNotNull(resource);
        checkArgument(resource instanceof DiscreteResource || resource instanceof ContinuousResource);

        if (resource instanceof DiscreteResource) {
            // check if already consumed
            return discreteStore.isAvailable((DiscreteResource) resource);
        } else {
            return continuousStore.isAvailable((ContinuousResource) resource);
        }
    }

    // computational complexity: O(n + m) where n is the number of entries in discreteConsumers
    // and m is the number of allocations for all continuous resources
    @Override
    public Collection<Resource> getResources(ResourceConsumer consumer) {
        checkNotNull(consumer);

        // NOTE: getting all entries may become performance bottleneck
        // TODO: revisit for better backend data structure
        Stream<DiscreteResource> discrete = discreteStore.getResources(consumer.consumerId());
        Stream<ContinuousResource> continuous = continuousStore.getResources(consumer.consumerId());

        return Stream.concat(discrete, continuous).collect(Collectors.toList());
    }

    // computational complexity: O(1)
    @Override
    public Set<Resource> getChildResources(DiscreteResourceId parent) {
        checkNotNull(parent);

        return ImmutableSet.<Resource>builder()
                .addAll(discreteStore.getChildResources(parent))
                .addAll(continuousStore.getChildResources(parent))
                .build();
    }

    @Override
    public <T> Set<Resource> getChildResources(DiscreteResourceId parent, Class<T> cls) {
        checkNotNull(parent);
        checkNotNull(cls);

        return ImmutableSet.<Resource>builder()
                .addAll(discreteStore.getChildResources(parent, cls))
                .addAll(continuousStore.getChildResources(parent, cls))
                .build();
    }

    // computational complexity: O(n) where n is the number of the children of the parent
    @Override
    public <T> Collection<Resource> getAllocatedResources(DiscreteResourceId parent, Class<T> cls) {
        checkNotNull(parent);
        checkNotNull(cls);

        Stream<DiscreteResource> discrete = discreteStore.getAllocatedResources(parent, cls);
        Stream<ContinuousResource> continuous = continuousStore.getAllocatedResources(parent, cls);

        return Stream.concat(discrete, continuous).collect(Collectors.toList());
    }

}
