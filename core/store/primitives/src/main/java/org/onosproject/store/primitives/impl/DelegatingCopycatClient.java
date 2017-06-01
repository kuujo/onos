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
package org.onosproject.store.primitives.impl;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.CopycatMetadata;
import io.atomix.copycat.client.session.CopycatSession;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * {@code CopycatClient} that merely delegates control to
 * another CopycatClient.
 */
public class DelegatingCopycatClient implements CopycatClient {

    protected final CopycatClient client;

    DelegatingCopycatClient(CopycatClient client) {
        this.client = client;
    }

    @Override
    public String id() {
        return client.id();
    }

    @Override
    public CopycatMetadata metadata() {
        return client.metadata();
    }

    @Override
    public ThreadContext context() {
        return client.context();
    }

    @Override
    public Serializer serializer() {
        return client.serializer();
    }

    @Override
    public CopycatSession.Builder sessionBuilder() {
        return client.sessionBuilder();
    }

    @Override
    public CompletableFuture<CopycatClient> connect(Collection<Address> members) {
        return client.connect(members);
    }

    @Override
    public CompletableFuture<Void> close() {
        return client.close();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id())
                .toString();
    }
}