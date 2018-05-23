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

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.service.RaftServiceContext;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.concurrent.AtomixThreadFactory;
import io.atomix.utils.concurrent.SingleThreadContextFactory;
import org.junit.Test;
import org.onosproject.store.service.Task;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.onosproject.store.primitives.resources.impl.AtomixPrimitiveTypes.WORK_QUEUE;
import static org.onosproject.store.primitives.resources.impl.AtomixWorkQueueOperations.ADD;
import static org.onosproject.store.primitives.resources.impl.AtomixWorkQueueOperations.TAKE;

/**
 * Work queue service test.
 */
public class AtomixWorkQueueServiceTest {
    @Test
    public void testSnapshot() throws Exception {
        RaftServiceContext context = mock(RaftServiceContext.class);
        expect(context.serviceType()).andReturn(WORK_QUEUE).anyTimes();
        expect(context.serviceName()).andReturn("test").anyTimes();
        expect(context.serviceId()).andReturn(PrimitiveId.from(1)).anyTimes();

        RaftContext server = mock(RaftContext.class);
        expect(server.getProtocol()).andReturn(mock(RaftServerProtocol.class));

        replay(context, server);

        AtomixWorkQueueService service = new AtomixWorkQueueService();
        service.init(context);

        RaftSession session = new RaftSession(
                SessionId.from(1),
                MemberId.from("1"),
                "test",
                WORK_QUEUE,
                ReadConsistency.LINEARIZABLE,
                100,
                5000,
                System.currentTimeMillis(),
                service.serializer(),
                context,
                server,
                new SingleThreadContextFactory(new AtomixThreadFactory()));

        service.add(new DefaultCommit<>(
                2,
                ADD,
                new AtomixWorkQueueOperations.Add(Collections.singletonList("Hello world!".getBytes())),
                session,
                System.currentTimeMillis()));

        Buffer buffer = HeapBuffer.allocate();
        service.backup(new DefaultBackupOutput(buffer, service.serializer()));

        service = new AtomixWorkQueueService();
        service.init(context);
        service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

        Collection<Task<byte[]>> value = service.take(new DefaultCommit<>(
                2,
                TAKE,
                new AtomixWorkQueueOperations.Take(1),
                session,
                System.currentTimeMillis()));
        assertNotNull(value);
        assertEquals(1, value.size());
        assertArrayEquals("Hello world!".getBytes(), value.iterator().next().payload());
    }
}
