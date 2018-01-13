/*
 * Copyright 2014-present Open Networking Foundation
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
package org.onosproject.store.flow.impl;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onosproject.cluster.GroupLeadershipServiceAdapter;
import org.onosproject.cluster.Leader;
import org.onosproject.cluster.Leadership;
import org.onosproject.cluster.LeadershipEvent;
import org.onosproject.cluster.LeadershipEventListener;
import org.onosproject.cluster.MembershipGroupId;
import org.onosproject.cluster.NodeId;
import org.onosproject.common.event.impl.TestEventDispatcher;
import org.onosproject.core.Version;
import org.onosproject.net.DeviceId;
import org.onosproject.store.flow.ReplicaInfoEvent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicaInfoManagerTest {

    private static final DeviceId DID1 = DeviceId.deviceId("of:1");
    private static final DeviceId DID2 = DeviceId.deviceId("of:2");
    private static final NodeId NID1 = new NodeId("foo");
    private static final NodeId NID2 = new NodeId("bar");

    private TestLeadershipService leadershipService;
    private ReplicaInfoManager manager;

    @Before
    public void setUp() throws Exception {
        leadershipService = new TestLeadershipService();
        manager = new TestReplicaInfoManager();
        manager.leadershipService = leadershipService;
        manager.activate();
    }

    @After
    public void tearDown() throws Exception {
        manager.deactivate();
    }

    @Test
    public void testMastershipTopics() throws Exception {
        assertEquals("device:of:1", manager.createDeviceMastershipTopic(DID1));
        assertEquals(DID1, manager.extractDeviceIdFromTopic("device:of:1"));
        assertTrue(manager.isDeviceMastershipTopic("device:of:1"));
        assertFalse(manager.isDeviceMastershipTopic("foo:bar"));
        assertFalse(manager.isDeviceMastershipTopic("foo:bar"));
        assertFalse(manager.isDeviceMastershipTopic("foobarbaz"));
        assertFalse(manager.isDeviceMastershipTopic("foobarbaz"));
    }

    @Test
    public void testReplicaEvents() throws Exception {
        Queue<ReplicaInfoEvent> events = new ArrayBlockingQueue<>(2);
        manager.addListener(events::add);

        Leadership leadership = new Leadership(
            manager.createDeviceMastershipTopic(DID1),
            new Leader(NID2, 2, 1),
            Lists.newArrayList(NID2, NID1));

        leadershipService.leaderships.put(manager.createDeviceMastershipTopic(DID1), leadership);
        leadershipService.post(new LeadershipEvent(
            LeadershipEvent.Type.LEADER_AND_CANDIDATES_CHANGED,
            leadership,
            MembershipGroupId.from(Version.version("1.0.0"))));

        ReplicaInfoEvent event = events.remove();
        assertEquals(ReplicaInfoEvent.Type.MASTER_CHANGED, event.type());
        assertEquals(NID2, event.replicaInfo().master().get());
        assertEquals(1, event.replicaInfo().backups().size());

        event = events.remove();
        assertEquals(ReplicaInfoEvent.Type.BACKUPS_CHANGED, event.type());
        assertEquals(NID2, event.replicaInfo().master().get());
        assertEquals(1, event.replicaInfo().backups().size());

        assertEquals(NID2, manager.getReplicaInfoFor(DID1).master().get());
        assertEquals(1, manager.getReplicaInfoFor(DID1).backups().size());

        leadership = new Leadership(
            manager.createDeviceMastershipTopic(DID1),
            new Leader(NID1, 1, 1),
            Lists.newArrayList(NID1, NID2));

        leadershipService.leaderships.put(manager.createDeviceMastershipTopic(DID1), leadership);
        leadershipService.post(new LeadershipEvent(
            LeadershipEvent.Type.CANDIDATES_CHANGED,
            leadership,
            MembershipGroupId.from(Version.version("1.0.0"))));

        event = events.remove();
        assertEquals(ReplicaInfoEvent.Type.BACKUPS_CHANGED, event.type());
        assertEquals(NID1, event.replicaInfo().master().get());
        assertEquals(1, event.replicaInfo().backups().size());

        assertEquals(NID1, manager.getReplicaInfoFor(DID1).master().get());
        assertEquals(1, manager.getReplicaInfoFor(DID1).backups().size());
    }

    private class TestReplicaInfoManager extends ReplicaInfoManager {
        TestReplicaInfoManager() {
            eventDispatcher = new TestEventDispatcher();
        }
    }

    private class TestLeadershipService extends GroupLeadershipServiceAdapter {
        private final Map<String, Leadership> leaderships = Maps.newConcurrentMap();
        private final Set<LeadershipEventListener> listeners = Sets.newConcurrentHashSet();

        @Override
        public Leadership getLeadership(String topic) {
            return leaderships.get(topic);
        }

        @Override
        public void addListener(LeadershipEventListener listener) {
            listeners.add(listener);
        }

        void post(LeadershipEvent event) {
            listeners.forEach(l -> l.event(event));
        }
    }
}
