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
package org.onosproject.store.cluster.messaging.impl;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onosproject.cluster.ClusterMetadata;
import org.onosproject.cluster.ClusterMetadataEventListener;
import org.onosproject.cluster.ClusterMetadataService;
import org.onosproject.cluster.ClusterServiceAdapter;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.DefaultControllerNode;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.HybridLogicalClockService;
import org.onosproject.core.HybridLogicalTime;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.store.cluster.messaging.MessageSubject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.onlab.junit.TestTools.findAvailablePort;

/**
 * Unit tests for NettyMessaging.
 */
public class ClusterCommunicationManagerTest {

    HybridLogicalClockService testClockService = new HybridLogicalClockService() {
        AtomicLong counter = new AtomicLong();

        @Override
        public HybridLogicalTime timeNow() {
            return new HybridLogicalTime(counter.incrementAndGet(), 0);
        }

        @Override
        public void recordEventTime(HybridLogicalTime time) {
        }
    };

    private static final String DUMMY_NAME = "node";

    ClusterCommunicationManager clusterCommunicator1;
    ClusterCommunicationManager clusterCommunicator2;

    ControllerNode node1 = new DefaultControllerNode(NodeId.nodeId("1"), "localhost", findAvailablePort(5001));
    ControllerNode node2 = new DefaultControllerNode(NodeId.nodeId("2"), "localhost", findAvailablePort(5002));

    @Before
    public void setUp() throws Exception {
        NettyMessagingManager messagingService1 = new NettyMessagingManager();
        messagingService1.clusterMetadataService = dummyMetadataService(DUMMY_NAME, node1);
        messagingService1.clockService = testClockService;
        messagingService1.activate();

        clusterCommunicator1 = new ClusterCommunicationManager(false);
        clusterCommunicator1.messagingService = messagingService1;
        clusterCommunicator1.clusterService = new ClusterServiceAdapter() {
            @Override
            public ControllerNode getLocalNode() {
                return node1;
            }

            @Override
            public ControllerNode getNode(NodeId nodeId) {
                return node2;
            }
        };
        clusterCommunicator1.activate();

        NettyMessagingManager messagingService2 = new NettyMessagingManager();
        messagingService2.clusterMetadataService = dummyMetadataService(DUMMY_NAME, node2);
        messagingService2.clockService = testClockService;
        messagingService2.activate();

        clusterCommunicator2 = new ClusterCommunicationManager(false);
        clusterCommunicator2.messagingService = messagingService2;
        clusterCommunicator2.clusterService = new ClusterServiceAdapter() {
            @Override
            public ControllerNode getLocalNode() {
                return node2;
            }

            @Override
            public ControllerNode getNode(NodeId nodeId) {
                return node1;
            }
        };
        clusterCommunicator2.activate();
    }

    /**
     * Returns a random String to be used as a test subject.
     *
     * @return string
     */
    private MessageSubject nextSubject() {
        return new MessageSubject(UUID.randomUUID().toString());
    }

    @After
    public void tearDown() throws Exception {
        if (clusterCommunicator1 != null) {
            clusterCommunicator1.deactivate();
            ((NettyMessagingManager) clusterCommunicator1.messagingService).deactivate();
        }

        if (clusterCommunicator2 != null) {
            clusterCommunicator2.deactivate();
            ((NettyMessagingManager) clusterCommunicator2.messagingService).deactivate();
        }
    }

    @Test
    public void testSendAsync() throws Exception {
        MessageSubject subject = nextSubject();

        CountDownLatch latch1 = new CountDownLatch(1);
        clusterCommunicator1.addSubscriber(subject, String::new, message -> {
            assertEquals("Hello world!", message);
            latch1.countDown();
        }, MoreExecutors.directExecutor());

        CountDownLatch latch2 = new CountDownLatch(1);
        clusterCommunicator2.unicast("Hello world!", subject, String::getBytes, node1.id())
            .whenComplete((r, e) -> {
                assertNull(e);
                latch2.countDown();
            });

        latch1.await(10, TimeUnit.SECONDS);
        latch2.await(10, TimeUnit.SECONDS);
    }

    @Test
    public void testSendAndReceive() throws Exception {
        MessageSubject subject = nextSubject();

        CountDownLatch latch1 = new CountDownLatch(1);
        clusterCommunicator1.addSubscriber(subject, String::new, message -> {
            assertEquals("hello", message);
            latch1.countDown();
            return "world!";
        }, String::getBytes, MoreExecutors.directExecutor());

        CountDownLatch latch2 = new CountDownLatch(1);
        clusterCommunicator2.sendAndReceive("hello", subject, String::getBytes, String::new, node1.id())
            .whenComplete((r, e) -> {
                assertNull(e);
                assertEquals("world!", r);
                latch2.countDown();
            });

        latch1.await(10, TimeUnit.SECONDS);
        latch2.await(10, TimeUnit.SECONDS);
    }

    private ClusterMetadataService dummyMetadataService(String name, ControllerNode localNode) {
        return new ClusterMetadataService() {
            @Override
            public ClusterMetadata getClusterMetadata() {
                return new ClusterMetadata(new ProviderId(DUMMY_NAME, DUMMY_NAME),
                    name, Sets.newHashSet(), Sets.newHashSet());
            }

            @Override
            public ControllerNode getLocalNode() {
                return localNode;
            }

            @Override
            public void addListener(ClusterMetadataEventListener listener) {
            }

            @Override
            public void removeListener(ClusterMetadataEventListener listener) {
            }
        };
    }
}
