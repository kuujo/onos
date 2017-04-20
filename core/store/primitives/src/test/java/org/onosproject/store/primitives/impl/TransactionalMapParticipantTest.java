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

import java.util.UUID;

import io.atomix.resource.ResourceType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.onosproject.store.primitives.TransactionId;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMap;
import org.onosproject.store.primitives.resources.impl.AtomixTestBase;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.IsolationLevel;

/**
 * Transactional map participant test.
 */
public abstract class TransactionalMapParticipantTest extends AtomixTestBase {

    @BeforeClass
    public static void preTestSetup() throws Throwable {
        createCopycatServers(3);
    }

    @AfterClass
    public static void postTestCleanup() throws Exception {
        clearTests();
    }

    @Override
    protected ResourceType resourceType() {
        return new ResourceType(AtomixConsistentMap.class);
    }

    protected abstract IsolationLevel isolationLevel();

    @SuppressWarnings("unchecked")
    protected AsyncConsistentMap<String, byte[]> createMap(String mapName) {
        AsyncConsistentMap asyncMap = createAtomixClient().getResource(mapName, AtomixConsistentMap.class).join();
        return asyncMap;
    }

    @SuppressWarnings("unchecked")
    protected TransactionalMapParticipant<String, byte[]> createParticipant(String mapName) {
        AsyncTransactionalMap asyncMap = createAtomixClient().getResource(mapName,
                AtomixConsistentMap.class).join();
        Transaction<MapRecord<String, byte[]>> transaction = new Transaction<>(
                TransactionId.from(UUID.randomUUID().toString()),
                isolationLevel(),
                asyncMap);
        return createParticipant(asyncMap, transaction);
    }

    protected abstract TransactionalMapParticipant<String, byte[]> createParticipant(AsyncConsistentMap<String, byte[]> asyncMap, Transaction<MapRecord<String, byte[]>> transaction);

}
