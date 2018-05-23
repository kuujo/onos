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

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.GET;
import static org.onosproject.store.primitives.resources.impl.AtomixAtomicCounterMapOperations.PUT;

/**
 * Atomic counter map service test.
 */
public class AtomixAtomicCounterMapServiceTest {
    @Test
    public void testSnapshot() throws Exception {
        AtomixAtomicCounterMapService service = new AtomixAtomicCounterMapService();
        service.put(new DefaultCommit<>(
                2,
                PUT,
                new AtomixAtomicCounterMapOperations.Put("foo", 1),
                mock(RaftSession.class),
                System.currentTimeMillis()));

        Buffer buffer = HeapBuffer.allocate();
        service.backup(new DefaultBackupOutput(buffer, service.serializer()));

        service = new AtomixAtomicCounterMapService();
        service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

        long value = service.get(new DefaultCommit<>(
                2,
                GET,
                new AtomixAtomicCounterMapOperations.Get("foo"),
                mock(RaftSession.class),
                System.currentTimeMillis()));
        assertEquals(1, value);
    }
}
