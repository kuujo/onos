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

import java.util.Collection;
import java.util.Collections;

import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import org.junit.Test;
import org.onlab.util.Match;
import org.onosproject.store.service.Versioned;

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentSetMultimapOperations.GET;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentSetMultimapOperations.PUT;

/**
 * Consistent set multimap service test.
 */
public class AtomixConsistentSetMultimapServiceTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshot() throws Exception {
        AtomixConsistentSetMultimapService service = new AtomixConsistentSetMultimapService();
        service.put(new DefaultCommit<>(
                2,
                PUT,
                new AtomixConsistentSetMultimapOperations.Put(
                        "foo", Collections.singletonList("Hello world!".getBytes()), Match.ANY),
                mock(RaftSession.class),
                System.currentTimeMillis()));

        Buffer buffer = HeapBuffer.allocate();
        service.backup(new DefaultBackupOutput(buffer, service.serializer()));

        service = new AtomixConsistentSetMultimapService();
        service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

        Versioned<Collection<? extends byte[]>> value = service.get(new DefaultCommit<>(
                2,
                GET,
                new AtomixConsistentSetMultimapOperations.Get("foo"),
                mock(RaftSession.class),
                System.currentTimeMillis()));
        assertNotNull(value);
        assertEquals(1, value.value().size());
        assertArrayEquals("Hello world!".getBytes(), value.value().iterator().next());
    }
}
