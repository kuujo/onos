/*
 * Copyright 2018-present Open Networking Foundation
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

import java.util.concurrent.atomic.AtomicLong;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.impl.RaftServiceManager;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.service.RaftServiceContext;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.concurrent.AtomixThreadFactory;
import io.atomix.utils.concurrent.SingleThreadContextFactory;
import io.atomix.utils.concurrent.ThreadContext;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.onosproject.store.primitives.resources.impl.AtomixPrimitiveTypes.LOCK;

/**
 * Distributed lock service test.
 */
public class AtomixDistributedLockServiceTest {
    @Test
    public void testSnapshot() throws Exception {
        AtomicLong index = new AtomicLong();
        RaftServiceContext context = mock(RaftServiceContext.class);
        expect(context.serviceType()).andReturn(LOCK).anyTimes();
        expect(context.serviceName()).andReturn("test").anyTimes();
        expect(context.serviceId()).andReturn(PrimitiveId.from(1)).anyTimes();
        expect(context.currentIndex()).andReturn(index.get()).anyTimes();
        expect(context.currentOperation()).andReturn(OperationType.COMMAND).anyTimes();

        RaftContext server = mock(RaftContext.class);
        expect(server.getProtocol()).andReturn(mock(RaftServerProtocol.class)).anyTimes();
        RaftServiceManager manager = mock(RaftServiceManager.class);
        expect(manager.executor()).andReturn(mock(ThreadContext.class)).anyTimes();
        expect(server.getServiceManager()).andReturn(manager).anyTimes();

        replay(context, server);

        AtomixDistributedLockService service = new AtomixDistributedLockService();
        service.init(context);

        RaftSession session = new RaftSession(
            SessionId.from(1),
            MemberId.from("1"),
            "test",
            LOCK,
            ReadConsistency.LINEARIZABLE,
            100,
            5000,
            System.currentTimeMillis(),
            service.serializer(),
            context,
            server,
            new SingleThreadContextFactory(new AtomixThreadFactory()));
        session.open();

        service.lock(new DefaultCommit<>(
            index.incrementAndGet(),
            AtomixDistributedLockOperations.LOCK,
            new AtomixDistributedLockOperations.Lock(1, 0),
            session,
            System.currentTimeMillis()));

        Buffer buffer = HeapBuffer.allocate();
        service.backup(new DefaultBackupOutput(buffer, service.serializer()));

        service = new AtomixDistributedLockService();
        service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

        service.unlock(new DefaultCommit<>(
            index.incrementAndGet(),
            AtomixDistributedLockOperations.UNLOCK,
            new AtomixDistributedLockOperations.Unlock(1),
            session,
            System.currentTimeMillis()));
    }
}
