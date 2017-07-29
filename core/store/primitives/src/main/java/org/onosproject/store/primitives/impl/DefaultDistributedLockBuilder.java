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
package org.onosproject.store.primitives.impl;

import org.onosproject.store.primitives.DistributedLock;
import org.onosproject.store.primitives.DistributedPrimitiveCreator;
import org.onosproject.store.service.AsyncDistributedLock;
import org.onosproject.store.service.DistributedLockBuilder;

/**
 * Default distributed lock builder.
 */
public class DefaultDistributedLockBuilder extends DistributedLockBuilder {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultDistributedLockBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public DistributedLock build() {
        return buildAsyncLock().asDistributedLock();
    }

    @Override
    public AsyncDistributedLock buildAsyncLock() {
        AsyncDistributedLock lock = primitiveCreator.newAsyncLock(name());
        if (reentrant) {
            lock = new ReentrantDistributedLock(lock);
        }
        return lock;
    }
}
