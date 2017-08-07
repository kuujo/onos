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
package org.onosproject.store.primitives;

import java.util.concurrent.CompletableFuture;

import org.onosproject.store.service.AsyncAtomicIdGenerator;
import org.onosproject.store.service.AtomicIdGenerator;
import org.onosproject.store.service.Synchronous;

/**
 * Default implementation for a {@code AtomicIdGenerator} backed by a {@link AsyncAtomicIdGenerator}.
 */
public class DefaultAtomicIdGenerator extends Synchronous<AsyncAtomicIdGenerator> implements AtomicIdGenerator {

    private final AsyncAtomicIdGenerator asyncIdGenerator;
    private final long operationTimeoutMillis;

    public DefaultAtomicIdGenerator(AsyncAtomicIdGenerator asyncIdGenerator, long operationTimeoutMillis) {
        super(asyncIdGenerator);
        this.asyncIdGenerator = asyncIdGenerator;
        this.operationTimeoutMillis = operationTimeoutMillis;
    }

    @Override
    public long nextId() {
        return complete(asyncIdGenerator.nextId());
    }

    private <T> T complete(CompletableFuture<T> future) {
        return complete(future, operationTimeoutMillis);
    }
}
