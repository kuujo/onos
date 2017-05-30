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
package org.onosproject.store.primitives.resources.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.atomix.copycat.client.CommunicationStrategy;
import io.atomix.copycat.client.CopycatClient;
import org.onosproject.store.primitives.impl.Managed;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Copycat primitive client.
 */
public class CopycatPrimitiveClient implements Managed<Void> {
    private final String name;
    private final String type;
    private final CopycatClient client;
    private final CommunicationStrategy communicationStrategy;
    private final AtomicBoolean open = new AtomicBoolean();

    public CopycatPrimitiveClient(
            String name,
            String type,
            CopycatClient client,
            CommunicationStrategy communicationStrategy) {
        this.name = checkNotNull(name);
        this.type = checkNotNull(type);
        this.client = checkNotNull(client);
        this.communicationStrategy = checkNotNull(communicationStrategy);
    }

    @Override
    public CompletableFuture<Void> open() {
        if (open.compareAndSet(false, true)) {
            client.onStateChange(this::onStateChange);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Handles a Copycat client state change.
     *
     * @param state the updated Copycat client state
     */
    private void onStateChange(CopycatClient.State state) {
        if (state == CopycatClient.State.CLOSED) {

        }
    }

    @Override
    public CompletableFuture<Void> close() {
        return null;
    }

    @Override
    public boolean isOpen() {
        return false;
    }
}
