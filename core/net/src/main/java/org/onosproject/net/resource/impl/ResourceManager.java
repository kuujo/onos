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
package org.onosproject.net.resource.impl;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onlab.util.Tools;
import org.onosproject.event.AbstractListenerManager;
import org.onosproject.net.resource.DiscreteResourceId;
import org.onosproject.net.resource.Resource;
import org.onosproject.net.resource.ResourceAdminService;
import org.onosproject.net.resource.ResourceAllocation;
import org.onosproject.net.resource.ResourceCommitStatus;
import org.onosproject.net.resource.ResourceConsumer;
import org.onosproject.net.resource.ResourceEvent;
import org.onosproject.net.resource.ResourceId;
import org.onosproject.net.resource.ResourceListener;
import org.onosproject.net.resource.ResourceService;
import org.onosproject.net.resource.ResourceStore;
import org.onosproject.net.resource.ResourceStoreDelegate;
import org.onosproject.net.resource.ResourceTransaction;
import org.onosproject.net.resource.ResourceTransactionContext;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.onosproject.security.AppGuard.checkPermission;
import static org.onosproject.security.AppPermission.Type.RESOURCE_READ;
import static org.onosproject.security.AppPermission.Type.RESOURCE_WRITE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * An implementation of ResourceService.
 */
@Component(immediate = true)
@Service
@Beta
public final class ResourceManager extends AbstractListenerManager<ResourceEvent, ResourceListener>
        implements ResourceService, ResourceAdminService {

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ResourceStore store;

    private final Logger log = getLogger(getClass());

    private final ResourceStoreDelegate delegate = new InternalStoreDelegate();

    @Activate
    public void activate() {
        store.setDelegate(delegate);
        eventDispatcher.addSink(ResourceEvent.class, listenerRegistry);

        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        store.unsetDelegate(delegate);
        eventDispatcher.removeSink(ResourceEvent.class);

        log.info("Stopped");
    }

    @Override
    public ResourceTransaction newTransaction() {
        return new ResourceManagerTransaction(store.newTransaction());
    }

    @Override
    public List<ResourceAllocation> allocate(ResourceConsumer consumer,
                                             List<? extends Resource> resources) {
        checkPermission(RESOURCE_WRITE);
        checkNotNull(consumer);
        checkNotNull(resources);

        for (;;) {
            try (ResourceTransactionContext tx = store.newTransaction()) {
                tx.allocate(resources, consumer);
                ResourceCommitStatus status = tx.commit();
                if (status == ResourceCommitStatus.SUCCEEDED) {
                    return resources.stream()
                            .map(x -> new ResourceAllocation(x, consumer))
                            .collect(Collectors.toList());
                } else if (status != ResourceCommitStatus.FAILED_CONCURRENT_TRANSACTION) {
                    return ImmutableList.of();
                }
            }
        }
    }

    @Override
    public List<ResourceAllocation> update(ResourceConsumer consumer,
            List<? extends Resource> resources) {
        for (;;) {
            try (ResourceTransaction transaction = newTransaction()) {
                // Get the list of current resource allocations
                Collection<ResourceAllocation> resourceAllocations =
                        transaction.getResourceAllocations(consumer);

                // Get the list of resources already allocated from resource allocations
                List<Resource> resourcesAllocated =
                        resourceAllocations.stream()
                                .map(ResourceAllocation::resource)
                                .collect(Collectors.toList());

                // Get the list of resource ids for resources already allocated
                List<ResourceId> idsResourcesAllocated =
                        resourcesAllocated.stream()
                                .map(Resource::id)
                                .collect(Collectors.toList());

                // Get the list of resource ids for resources being updated
                List<ResourceId> idsResources =
                        resources.stream()
                                .map(Resource::id)
                                .collect(Collectors.toList());

                // Create the list of resources to be added, meaning their key is not
                // present in the resources already allocated
                List<Resource> resourcesToAdd =
                        resources.stream()
                                .filter(r -> !idsResourcesAllocated.contains(r.id()))
                                .collect(Collectors.toList());

                // Create the list of resources to be removed, meaning their key
                // is not present in the resource updates
                List<ResourceAllocation> allocationsToRelease =
                        resourceAllocations.stream()
                                .filter(r -> !idsResources.contains(r.resource().id()))
                                .collect(Collectors.toList());

                // Resources to updated are all the new valid resources except the
                // resources to be added
                List<Resource> resourcesToUpdate = Lists.newArrayList(resources);
                resourcesToUpdate.removeAll(resourcesToAdd);

                // If there are no resources to update skip update procedures
                if (!resourcesToUpdate.isEmpty()) {
                    // Remove old resources that need to be updated
                    List<ResourceAllocation> resourceAllocationsToUpdate =
                            resourceAllocations.stream()
                                    .filter(rA -> resourcesToUpdate.stream()
                                            .map(Resource::id)
                                            .collect(Collectors.toList())
                                            .contains(rA.resource().id()))
                                    .collect(Collectors.toList());
                    transaction.release(resourceAllocationsToUpdate);

                    // Update resourcesToAdd with the list of both the new resources and
                    // the resources to update
                    resourcesToAdd.addAll(resourcesToUpdate);
                }

                // Allocate resources
                if (!resourcesToAdd.isEmpty()) {
                    transaction.allocate(consumer, resourcesToAdd);
                }

                // Release resources
                if (!allocationsToRelease.isEmpty()) {
                    transaction.release(allocationsToRelease);
                }

                // Commit the transaction
                ResourceCommitStatus status = transaction.commit();
                if (status == ResourceCommitStatus.SUCCEEDED) {
                    return Streams.concat(resourcesToAdd.stream()
                                    .map(x -> new ResourceAllocation(x, consumer)),
                            resourcesToUpdate.stream()
                                    .map(x -> new ResourceAllocation(x, consumer)))
                            .collect(Collectors.toList());
                } else if (status != ResourceCommitStatus.FAILED_CONCURRENT_TRANSACTION) {
                    return ImmutableList.of();
                }
            }
        }
    }

    @Override
    public boolean release(List<ResourceAllocation> allocations) {
        checkPermission(RESOURCE_WRITE);
        checkNotNull(allocations);

        for (;;) {
            try (ResourceTransactionContext tx = store.newTransaction()) {
                tx.release(allocations);
                ResourceCommitStatus status = tx.commit();
                if (status == ResourceCommitStatus.SUCCEEDED) {
                    return true;
                } else if (status != ResourceCommitStatus.FAILED_CONCURRENT_TRANSACTION) {
                    return false;
                }
            }
        }
    }

    @Override
    public boolean release(ResourceConsumer consumer) {
        checkNotNull(consumer);

        Collection<ResourceAllocation> allocations = getResourceAllocations(consumer);
        return release(ImmutableList.copyOf(allocations));
    }

    @Override
    public List<ResourceAllocation> getResourceAllocations(ResourceId id) {
        checkPermission(RESOURCE_READ);
        checkNotNull(id);

        return store.getResourceAllocations(id);
    }

    @Override
    public <T> Collection<ResourceAllocation> getResourceAllocations(DiscreteResourceId parent, Class<T> cls) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);
        checkNotNull(cls);

        // We access store twice in this method, then the store may be updated by others
        Collection<Resource> resources = store.getAllocatedResources(parent, cls);
        return resources.stream()
                .flatMap(resource -> store.getResourceAllocations(resource.id()).stream())
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Collection<ResourceAllocation> getResourceAllocations(ResourceConsumer consumer) {
        checkPermission(RESOURCE_READ);
        checkNotNull(consumer);

        Collection<Resource> resources = store.getResources(consumer);
        return resources.stream()
                .map(x -> new ResourceAllocation(x, consumer))
                .collect(Collectors.toList());
    }

    @Override
    public Set<Resource> getAvailableResources(DiscreteResourceId parent) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);

        Set<Resource> children = store.getChildResources(parent);
        return children.stream()
                // We access store twice in this method, then the store may be updated by others
                .filter(store::isAvailable)
                .collect(Collectors.toSet());
    }

    @Override
    public <T> Set<Resource> getAvailableResources(DiscreteResourceId parent, Class<T> cls) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);
        checkNotNull(cls);

        return store.getChildResources(parent, cls).stream()
                // We access store twice in this method, then the store may be updated by others
                .filter(store::isAvailable)
                .collect(Collectors.toSet());
    }

    @Override
    public <T> Set<T> getAvailableResourceValues(DiscreteResourceId parent, Class<T> cls) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);
        checkNotNull(cls);

        return store.getChildResources(parent, cls).stream()
                // We access store twice in this method, then the store may be updated by others
                .filter(store::isAvailable)
                .map(x -> x.valueAs(cls))
                .flatMap(Tools::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Resource> getRegisteredResources(DiscreteResourceId parent) {
        checkPermission(RESOURCE_READ);
        checkNotNull(parent);

        return store.getChildResources(parent);
    }

    @Override
    public boolean isAvailable(Resource resource) {
        checkPermission(RESOURCE_READ);
        checkNotNull(resource);

        return store.isAvailable(resource);
    }

    @Override
    public boolean register(List<? extends Resource> resources) {
        checkNotNull(resources);

        for (;;) {
            try (ResourceTransactionContext tx = store.newTransaction()) {
                tx.register(resources);
                ResourceCommitStatus status = tx.commit();
                if (status == ResourceCommitStatus.SUCCEEDED) {
                    return true;
                } else if (status != ResourceCommitStatus.FAILED_CONCURRENT_TRANSACTION) {
                    return false;
                }
            }
        }
    }

    @Override
    public boolean unregister(List<? extends ResourceId> ids) {
        checkNotNull(ids);

        for (;;) {
            try (ResourceTransactionContext tx = store.newTransaction()) {
                tx.unregister(ids);
                ResourceCommitStatus status = tx.commit();
                if (status == ResourceCommitStatus.SUCCEEDED) {
                    return true;
                } else if (status != ResourceCommitStatus.FAILED_CONCURRENT_TRANSACTION) {
                    return false;
                }
            }
        }
    }

    private class InternalStoreDelegate implements ResourceStoreDelegate {
        @Override
        public void notify(ResourceEvent event) {
            post(event);
        }
    }
}
