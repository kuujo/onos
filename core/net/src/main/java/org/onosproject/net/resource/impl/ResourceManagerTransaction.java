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
package org.onosproject.net.resource.impl;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.onlab.util.GuavaCollectors;
import org.onlab.util.Tools;
import org.onosproject.net.resource.DiscreteResourceId;
import org.onosproject.net.resource.Resource;
import org.onosproject.net.resource.ResourceAllocation;
import org.onosproject.net.resource.ResourceCommitStatus;
import org.onosproject.net.resource.ResourceConsumer;
import org.onosproject.net.resource.ResourceId;
import org.onosproject.net.resource.ResourceTransaction;
import org.onosproject.net.resource.ResourceTransactionContext;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.onosproject.security.AppGuard.checkPermission;
import static org.onosproject.security.AppPermission.Type.RESOURCE_READ;
import static org.onosproject.security.AppPermission.Type.RESOURCE_WRITE;

/**
 * Resource transaction implementation.
 */
public class ResourceManagerTransaction implements ResourceTransaction {
    private final ResourceTransactionContext context;

    public ResourceManagerTransaction(ResourceTransactionContext context) {
        this.context = context;
    }

    @Override
    public ResourceCommitStatus commit() {
        return context.commit();
    }

    @Override
    public void abort() {
        context.abort();
    }

    @Override
    public List<ResourceAllocation> getResourceAllocations(ResourceId id) {
        checkPermission(RESOURCE_READ);
        checkNotNull(id);

        return context.getResourceAllocations(id);
    }

    @Override
    public <T> Collection<ResourceAllocation> getResourceAllocations(DiscreteResourceId parent, Class<T> cls) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);
        checkNotNull(cls);

        // We access store twice in this method, then the store may be updated by others
        Collection<Resource> resources = context.getAllocatedResources(parent, cls);
        return resources.stream()
                .flatMap(resource -> context.getResourceAllocations(resource.id()).stream())
                .collect(GuavaCollectors.toImmutableList());
    }

    @Override
    public Collection<ResourceAllocation> getResourceAllocations(ResourceConsumer consumer) {
        checkPermission(RESOURCE_READ);
        checkNotNull(consumer);

        Collection<Resource> resources = context.getResources(consumer);
        return resources.stream()
                .map(x -> new ResourceAllocation(x, consumer))
                .collect(Collectors.toList());
    }

    @Override
    public Set<Resource> getAvailableResources(DiscreteResourceId parent) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);

        Set<Resource> children = context.getChildResources(parent);
        return children.stream()
                // We access store twice in this method, then the store may be updated by others
                .filter(context::isAvailable)
                .collect(Collectors.toSet());
    }

    @Override
    public <T> Set<Resource> getAvailableResources(DiscreteResourceId parent, Class<T> cls) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);
        checkNotNull(cls);

        return context.getChildResources(parent, cls).stream()
                // We access store twice in this method, then the store may be updated by others
                .filter(context::isAvailable)
                .collect(Collectors.toSet());
    }

    @Override
    public <T> Set<T> getAvailableResourceValues(DiscreteResourceId parent, Class<T> cls) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);
        checkNotNull(cls);

        return context.getChildResources(parent, cls).stream()
                // We access store twice in this method, then the store may be updated by others
                .filter(context::isAvailable)
                .map(x -> x.valueAs(cls))
                .flatMap(Tools::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Resource> getRegisteredResources(DiscreteResourceId parent) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);
        return context.getChildResources(parent);
    }

    @Override
    public boolean isAvailable(Resource resource) {
        checkPermission(RESOURCE_READ);
        checkNotNull(resource);
        return context.isAvailable(resource);
    }

    @Override
    public void allocate(ResourceConsumer consumer, List<? extends Resource> resources) {
        checkPermission(RESOURCE_WRITE);
        checkNotNull(consumer);
        checkNotNull(resources);
        context.allocate(resources, consumer);
    }

    @Override
    public void release(List<ResourceAllocation> allocations) {
        checkPermission(RESOURCE_WRITE);
        checkNotNull(allocations);
        context.release(allocations);
    }

    @Override
    public void release(ResourceConsumer consumer) {
        checkNotNull(consumer);
        Collection<ResourceAllocation> allocations = getResourceAllocations(consumer);
        release(ImmutableList.copyOf(allocations));
    }

    @Override
    public void close() {
        context.close();
    }
}
