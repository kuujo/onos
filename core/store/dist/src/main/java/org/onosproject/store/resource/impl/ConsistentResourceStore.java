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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.Beta;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onlab.util.KryoNamespace;
import org.onosproject.net.resource.DiscreteResourceId;
import org.onosproject.net.resource.Resource;
import org.onosproject.net.resource.ResourceAllocation;
import org.onosproject.net.resource.ResourceConsumer;
import org.onosproject.net.resource.ResourceEvent;
import org.onosproject.net.resource.ResourceId;
import org.onosproject.net.resource.ResourceStore;
import org.onosproject.net.resource.ResourceStoreDelegate;
import org.onosproject.net.resource.ResourceTransactionContext;
import org.onosproject.store.AbstractStore;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ResourceStore using TransactionalMap.
 */
@Component(immediate = true)
@Service
@Beta
public class ConsistentResourceStore extends AbstractStore<ResourceEvent, ResourceStoreDelegate>
        implements ResourceStore {
    private static final Logger log = LoggerFactory.getLogger(ConsistentResourceStore.class);

    static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
            .register(KryoNamespaces.API)
            .register(UnifiedDiscreteResources.class)
            .register(new EncodableDiscreteResourcesSerializer(), EncodableDiscreteResources.class)
            .register(GenericDiscreteResources.class)
            .register(EmptyDiscreteResources.class)
            .register(new EncodedResourcesSerializer(), EncodedDiscreteResources.class)
            .register(ContinuousResourceAllocation.class)
            .register(PortNumberCodec.class)
            .register(VlanIdCodec.class)
            .register(MplsLabelCodec.class)
            .build());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    private ConsistentResourceStoreBase base;

    @Activate
    public void activate() {
        ConsistentDiscreteResourceSubStore discreteStore = new ConsistentDiscreteResourceSubStore(storageService);
        ConsistentContinuousResourceSubStore continuousStore = new ConsistentContinuousResourceSubStore(storageService);
        base = new ConsistentResourceStoreBase(discreteStore, continuousStore);
        log.info("Started");
    }

    @Override
    public ResourceTransactionContext newTransaction() {
        return new ConsistentResourceTransactionContext(storageService.transactionContextBuilder().build());
    }

    @Override
    public List<ResourceAllocation> getResourceAllocations(ResourceId id) {
        return base.getResourceAllocations(id);
    }

    @Override
    public boolean isAvailable(Resource resource) {
        return base.isAvailable(resource);
    }

    @Override
    public Collection<Resource> getResources(ResourceConsumer consumer) {
        return base.getResources(consumer);
    }

    @Override
    public Set<Resource> getChildResources(DiscreteResourceId parent) {
        return base.getChildResources(parent);
    }

    @Override
    public <T> Set<Resource> getChildResources(DiscreteResourceId parent, Class<T> cls) {
        return base.getChildResources(parent, cls);
    }

    @Override
    public <T> Collection<Resource> getAllocatedResources(DiscreteResourceId parent, Class<T> cls) {
        return base.getAllocatedResources(parent, cls);
    }
}
