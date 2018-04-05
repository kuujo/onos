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

import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.log.RaftLog;
import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.atomix.storage.StorageLevel;
import org.onosproject.store.primitives.impl.StorageNamespaces;
import org.onosproject.store.service.Serializer;

import static org.junit.Assert.assertEquals;

/**
 * Atomix log test.
 */
public class AtomixLogTest {
    //@Test
    public void testLog() throws Exception {
        RaftStorage storage = RaftStorage.newBuilder()
            .withPrefix("partition-1")
            .withStorageLevel(StorageLevel.DISK)
            .withDirectory("/Users/jordanhalterman/Projects/onos/test-logs/ha-1/10.192.19.102/1")
            .withSerializer(new AtomixSerializerAdapter(Serializer.using(StorageNamespaces.RAFT_STORAGE)))
            .withMaxSegmentSize(1024 * 64)
            .build();

        RaftLog log = storage.openLog();
        RaftLogReader reader = log.openReader(1);
        long nextIndex = reader.getFirstIndex();
        System.out.println(nextIndex);
        while (reader.hasNext()) {
            assertEquals(nextIndex++, reader.next().index());
        }
        System.out.println(reader.getCurrentIndex());
    }
}
