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

import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.session.CopycatSession;

/**
 * Copycat client that recovers sessions that are lost.
 */
public class RecoveringCopycatClient extends DelegatingCopycatClient {
    public RecoveringCopycatClient(CopycatClient client) {
        super(client);
    }

    @Override
    public CopycatSession.Builder sessionBuilder() {
        return new CopycatSession.Builder() {
            @Override
            public CopycatSession build() {
                CopycatSession.Builder builder = client.sessionBuilder()
                        .withName(name)
                        .withType(type)
                        .withCommunicationStrategy(communicationStrategy)
                        .withTimeout(timeout);
                return new RecoveringCopycatSession(builder);
            }
        };
    }
}
