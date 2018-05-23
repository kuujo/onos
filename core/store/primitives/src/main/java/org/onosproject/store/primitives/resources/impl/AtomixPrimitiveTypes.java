/*
 * Copyright 2018-present Open Networking Foundation
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
package org.onosproject.store.primitives.resources.impl;

import java.util.function.Supplier;

import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import org.onosproject.store.service.Ordering;

/**
 * Atomix primitive types.
 */
public enum AtomixPrimitiveTypes implements PrimitiveType {
    CONSISTENT_MAP(AtomixConsistentMapService::new),
    CONSISTENT_MULTIMAP(AtomixConsistentSetMultimapService::new),
    CONSISTENT_TREEMAP(AtomixConsistentTreeMapService::new),
    ATOMIC_COUNTER_MAP(AtomixAtomicCounterMapService::new),
    ATOMIC_COUNTER(AtomixCounterService::new),
    WORK_QUEUE(AtomixWorkQueueService::new),
    DOCUMENT_TREE(() -> new AtomixDocumentTreeService(Ordering.NATURAL)),
    LOCK(AtomixDistributedLockService::new),
    LEADER_ELECTOR(AtomixLeaderElectorService::new);

    private final Supplier<PrimitiveService> serviceSupplier;

    AtomixPrimitiveTypes(Supplier<PrimitiveService> serviceSupplier) {
        this.serviceSupplier = serviceSupplier;
    }

    @Override
    public String id() {
        return name();
    }

    @Override
    public PrimitiveService newService(ServiceConfig config) {
        return serviceSupplier.get();
    }

    @Override
    public DistributedPrimitiveBuilder newPrimitiveBuilder(
        String name, PrimitiveManagementService managementService) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DistributedPrimitiveBuilder newPrimitiveBuilder(
        String name, PrimitiveConfig config, PrimitiveManagementService managementService) {
        throw new UnsupportedOperationException();
    }
}
