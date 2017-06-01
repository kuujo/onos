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

import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.session.CopycatSession;

/**
 * Custom {@code CopycatClient} for injecting additional logic that runs before/after operation submission.
 */
public class RetryingCopycatClient extends DelegatingCopycatClient {
    private static final int DEFAULT_MAX_RETRIES = 5;
    private static final long DEFAULT_DELAY_BETWEEN_RETRIES_MILLIS = 100;

    private final int maxRetries;
    private final long delayBetweenRetriesMillis;

    public RetryingCopycatClient(CopycatClient client) {
        this(client, DEFAULT_MAX_RETRIES, DEFAULT_DELAY_BETWEEN_RETRIES_MILLIS);
    }

    public RetryingCopycatClient(CopycatClient client, int maxRetries, long delayBetweenRetriesMillis) {
        super(client);
        this.maxRetries = maxRetries;
        this.delayBetweenRetriesMillis = delayBetweenRetriesMillis;
    }

    @Override
    public CopycatSession.Builder sessionBuilder() {
        return new CopycatSession.Builder() {
            @Override
            public CopycatSession build() {
                CopycatSession session = client.sessionBuilder()
                        .withName(name)
                        .withType(type)
                        .withCommunicationStrategy(communicationStrategy)
                        .withTimeout(timeout)
                        .build();
                return new RetryingCopycatSession(session, maxRetries, delayBetweenRetriesMillis);
            }
        };
    }
}
