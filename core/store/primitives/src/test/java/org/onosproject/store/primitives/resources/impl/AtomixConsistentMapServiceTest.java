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
package org.onosproject.store.primitives.resources.impl;

import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import org.junit.Test;
import org.onosproject.store.service.Versioned;

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMapOperations.GET;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMapOperations.PUT;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMapOperations.Put;

/**
 * Consistent map service test.
 */
public class AtomixConsistentMapServiceTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshot() throws Exception {
        AtomixConsistentMapService service = new AtomixConsistentMapService();
        service.put(new DefaultCommit<>(
                2,
                PUT,
                new Put("foo", "Hello world!".getBytes()),
                mock(RaftSession.class),
                System.currentTimeMillis()));

        Buffer buffer = HeapBuffer.allocate();
        service.backup(new DefaultBackupOutput(buffer, service.serializer()));

        service = new AtomixConsistentMapService();
        service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

        Versioned<byte[]> value = service.get(new DefaultCommit<>(
                2,
                GET,
                new AtomixConsistentMapOperations.Get("foo"),
                mock(RaftSession.class),
                System.currentTimeMillis()));
        assertNotNull(value);
        assertArrayEquals("Hello world!".getBytes(), value.value());
    }
}
