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

import org.junit.Test;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.IsolationLevel;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Serializable map participant test.
 */
public class SerializableMapParticipantTest extends TransactionalMapParticipantTest {
    @Override
    protected IsolationLevel isolationLevel() {
        return IsolationLevel.SERIALIZABLE;
    }

    @Override
    protected TransactionalMapParticipant<String, byte[]> createParticipant(AsyncConsistentMap<String, byte[]> asyncMap, Transaction<MapRecord<String, byte[]>> transaction) {
        return new SerializableMapParticipant<>(asyncMap, transaction);
    }

    @Test
    public void testSerializableTransactionWriteConflict() throws Throwable {
        TransactionalMapParticipant<String, byte[]> participant1 = createParticipant("testSerializableTransactionWriteConflict");
        TransactionalMapParticipant<String, byte[]> participant2 = createParticipant("testSerializableTransactionWriteConflict");

        participant1.put("foo", "1".getBytes());
        participant1.put("bar", "2".getBytes());

        participant2.put("bar", "3".getBytes());

        assertTrue(participant1.prepare().join());
        assertFalse(participant2.prepare().join());
        participant1.commit().join();
    }

    @Test
    public void testSerializableTransactionReadConflict() throws Throwable {
        TransactionalMapParticipant<String, byte[]> participant1 = createParticipant("testSerializableTransactionReadConflict");
        TransactionalMapParticipant<String, byte[]> participant2 = createParticipant("testSerializableTransactionReadConflict");

        assertNull(participant1.get("foo"));
        assertNull(participant1.put("bar", "2".getBytes()));

        assertNull(participant2.put("foo", "1".getBytes()));
        assertTrue(participant2.prepare().join());

        assertFalse(participant1.prepare().join());

        participant2.commit().join();

        AsyncConsistentMap<String, byte[]> asyncMap = createMap("testSerializableTransactionReadConflict");
        assertArrayEquals("1".getBytes(), asyncMap.get("foo").join().value());
    }

    @Test
    public void testSerializableReadRead() throws Throwable {
        TransactionalMapParticipant<String, byte[]> participant1 = createParticipant("testSerializableReadRead");

        participant1.put("foo", "1".getBytes());
        assertTrue(participant1.prepare().join());
        participant1.commit().join();

        TransactionalMapParticipant<String, byte[]> participant2 = createParticipant("testSerializableReadRead");
        assertArrayEquals("1".getBytes(), participant2.get("foo"));
        assertTrue(participant2.prepare().join());
        participant2.commit().join();

        TransactionalMapParticipant<String, byte[]> participant3 = createParticipant("testSerializableReadRead");
        assertArrayEquals("1".getBytes(), participant3.get("foo"));
        assertTrue(participant3.prepare().join());
        participant3.commit().join();
    }

    @Test
    public void testSerializableRollback() throws Throwable {
        TransactionalMapParticipant<String, byte[]> participant1 = createParticipant("testSerializableRollback");
        participant1.put("foo", "1".getBytes());
        assertTrue(participant1.prepare().join());
        participant1.rollback().join();

        AsyncConsistentMap<String, byte[]> asyncMap = createMap("testSerializableRollback");
        assertNull(asyncMap.get("foo").join());
    }
}
