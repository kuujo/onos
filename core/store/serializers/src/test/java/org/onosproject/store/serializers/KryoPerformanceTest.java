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
package org.onosproject.store.serializers;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.math.Stats;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.onlab.packet.ChassisId;
import org.onlab.packet.Ip4Address;
import org.onlab.packet.Ip4Prefix;
import org.onlab.packet.Ip6Address;
import org.onlab.packet.Ip6Prefix;
import org.onlab.packet.IpAddress;
import org.onlab.packet.IpPrefix;
import org.onlab.packet.MacAddress;
import org.onlab.packet.VlanId;
import org.onlab.util.Bandwidth;
import org.onlab.util.Frequency;
import org.onosproject.cluster.NodeId;
import org.onosproject.cluster.RoleInfo;
import org.onosproject.core.DefaultApplicationId;
import org.onosproject.core.GroupId;
import org.onosproject.mastership.MastershipTerm;
import org.onosproject.net.ChannelSpacing;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DefaultAnnotations;
import org.onosproject.net.DefaultDevice;
import org.onosproject.net.DefaultLink;
import org.onosproject.net.DefaultPort;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.GridType;
import org.onosproject.net.HostLocation;
import org.onosproject.net.Link;
import org.onosproject.net.LinkKey;
import org.onosproject.net.MarkerResource;
import org.onosproject.net.PortNumber;
import org.onosproject.net.SparseAnnotations;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowId;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleBatchEntry;
import org.onosproject.net.intent.IntentId;
import org.onosproject.net.intent.constraint.AnnotationConstraint;
import org.onosproject.net.intent.constraint.BandwidthConstraint;
import org.onosproject.net.intent.constraint.LatencyConstraint;
import org.onosproject.net.intent.constraint.LinkTypeConstraint;
import org.onosproject.net.intent.constraint.ObstacleConstraint;
import org.onosproject.net.intent.constraint.WaypointConstraint;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.net.resource.ResourceAllocation;
import org.onosproject.net.resource.ResourceConsumerId;
import org.onosproject.net.resource.Resources;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.onosproject.net.DeviceId.deviceId;
import static org.onosproject.net.PortNumber.portNumber;

public class KryoPerformanceTest {

    private static final ProviderId PID = new ProviderId("of", "foo");
    private static final ProviderId PIDA = new ProviderId("of", "foo", true);
    private static final DeviceId DID1 = deviceId("of:foo");
    private static final DeviceId DID2 = deviceId("of:bar");
    private static final PortNumber P1 = portNumber(1);
    private static final PortNumber P2 = portNumber(2);
    private static final ConnectPoint CP1 = new ConnectPoint(DID1, P1);
    private static final ConnectPoint CP2 = new ConnectPoint(DID2, P2);
    private static final String MFR = "whitebox";
    private static final String HW = "1.1.x";
    private static final String SW1 = "3.8.1";
    private static final String SN = "43311-12345";
    private static final ChassisId CID = new ChassisId();
    private static final Device DEV1 = new DefaultDevice(PID, DID1, Device.Type.SWITCH, MFR, HW,
            SW1, SN, CID);
    private static final SparseAnnotations A1 = DefaultAnnotations.builder()
            .set("A1", "a1")
            .set("B1", "b1")
            .build();
    private static final SparseAnnotations A1_2 = DefaultAnnotations.builder()
            .remove("A1")
            .set("B3", "b3")
            .build();
    private static final VlanId VLAN1 = VlanId.vlanId((short) 100);

    private StoreSerializer serializer;
    private ByteBuffer buffer;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        serializer = StoreSerializer.using(KryoNamespaces.API);
        buffer = ByteBuffer.allocate(1024 * 1024);
    }

    @After
    public void tearDown() throws Exception {
    }

    private byte[] serialize(Object object) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        serializer.encode(object, buffer);
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private <T> void testBytesEqual(T expected, T actual) {
        byte[] expectedBytes = serialize(expected);
        byte[] actualBytes = serialize(actual);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    private void testPerformance(String name, Object object) {
        testPerformance(name, object, 10000000, 3);
    }

    private void testPerformance(String name, Object object, int count, int iterations) {
        testSerializePerformance(name, object, count, iterations);
        testDeserializePerformance(name, object, count, iterations);
        testCopyPerformance(name, object, count, iterations);
    }

    private void testSerializePerformance(String name, Object object, int count, int iterations) {
        System.out.println("testSerializePerformance (name=" + name + ", count=" + count + "):");
        runIterations(count, iterations, () -> {
            buffer.clear();
        }, () -> {
            for (int i = 0; i < count; i++) {
                serializer.encode(object, buffer);
                buffer.clear();
            }
        });
    }

    private void testDeserializePerformance(String name, Object object, int count, int iterations) {
        System.out.println("testDeserializePerformance (name=" + name + ", count=" + count + "):");
        runIterations(count, iterations, () -> {
            buffer.clear();
            serializer.encode(object, buffer);
            buffer.flip();
        }, () -> {
            for (int i = 0; i < count; i++) {
                serializer.decode(buffer);
                buffer.rewind();
            }
        });
    }

    private void testCopyPerformance(String name, Object object, int count, int iterations) {
        System.out.println("testCopyPerformance (name=" + name + ", count=" + count + "):");
        runIterations(count, iterations, () -> {
            buffer.clear();
        }, () -> {
            for (int i = 0; i < count; i++) {
                serializer.encode(object, buffer);
                buffer.flip();
                serializer.decode(buffer);
                buffer.clear();
            }
        });
    }

    private void runIterations(int count, int iterations, Runnable setup, Runnable iteration) {
        List<Long> runTimes = Lists.newArrayList();
        for (int i = 1; i <= iterations; i++) {
            long startTime = System.nanoTime();
            iteration.run();
            long endTime = System.nanoTime();
            long runTime = endTime - startTime;
            printIteration(runTime, count, i);
            runTimes.add(runTime);
        }
        printStats(runTimes, count);
    }

    private void printIteration(long runTime, int count, int iteration) {
        System.out.println("iteration " + iteration + ": " + toMillis(runTime) + " milliseconds (" + toObjectsPerSecond(runTime, count) + " objects per second)");
    }

    private void printStats(List<Long> runTimes, int count) {
        Stats stats = Stats.of(runTimes);
        System.out.println("min: " + toObjectsPerSecond(stats.min(), count) + "/sec, max: " + toObjectsPerSecond(stats.max(), count) + "/sec, mean: " + toObjectsPerSecond(stats.mean(), count) + "/sec, variance: " + toObjectsPerSecond(stats.populationVariance(), count) + ", stddev: " + toObjectsPerSecond(stats.populationStandardDeviation(), count));
        System.out.println();
    }

    private long toMillis(long nanos) {
        return Duration.ofNanos(nanos).toMillis();
    }

    private double toObjectsPerSecond(double nanos, int count) {
        return toObjectsPerSecond((long) nanos, count);
    }

    private double toObjectsPerSecond(long nanos, int count) {
        return count / (toMillis(nanos) / 1000d);
    }

    @Test
    public void testConnectPoint() {
        testPerformance("ConnectPoint", new ConnectPoint(DID1, P1));
    }

    @Test
    public void testDefaultLink() {
        testPerformance("DefaultLink1", DefaultLink.builder()
                .providerId(PID)
                .src(CP1)
                .dst(CP2)
                .type(Link.Type.DIRECT)
                .build());
        testPerformance("DefaultLink2", DefaultLink.builder()
                .providerId(PID)
                .src(CP1)
                .dst(CP2)
                .type(Link.Type.DIRECT)
                .annotations(A1)
                .build());
    }

    @Test
    public void testDefaultPort() {
        testPerformance("DefaultPort1", new DefaultPort(DEV1, P1, true));
        testPerformance("DefaultPort2", new DefaultPort(DEV1, P1, true, A1_2));
    }

    @Test
    public void testDeviceId() {
        testPerformance("DeviceId", DID1);
    }

    @Test
    public void testImmutableMap() {
        testPerformance("RegularImmutableMap", ImmutableMap.of(DID1, DEV1, DID2, DEV1));
        testPerformance("RegularImmutableMap.EMPTY", ImmutableMap.of());
    }

    @Test
    public void testImmutableSet() {
        testPerformance("RegularImmutableSet", ImmutableSet.of(DID1, DID2));
        testPerformance("SingletonImmutableSet", ImmutableSet.of(DID1));
        testPerformance("RegularImmutableSet.EMPTY", ImmutableSet.of());
    }

    @Test
    public void testImmutableList() {
        testBytesEqual(ImmutableList.of(DID1, DID2), ImmutableList.of(DID1, DID2, DID1, DID2).subList(0, 2));
        testPerformance("ImmutableList.SubList", ImmutableList.of(DID1, DID2, DID1, DID2).subList(0, 2));
        testPerformance("RegularImmutableList", ImmutableList.of(DID1, DID2));
        testPerformance("RegularImmutableList.EMPTY", ImmutableList.of());
    }

    @Test
    public void testFlowRuleBatchEntry() {
        final FlowRule rule1 =
                DefaultFlowRule.builder()
                        .forDevice(DID1)
                        .withSelector(DefaultTrafficSelector.emptySelector())
                        .withTreatment(DefaultTrafficTreatment.emptyTreatment())
                        .withPriority(0)
                        .fromApp(new DefaultApplicationId(1, "1"))
                        .makeTemporary(1)
                        .build();

        final FlowRuleBatchEntry entry1 =
                new FlowRuleBatchEntry(FlowRuleBatchEntry.FlowRuleOperation.ADD, rule1);
        final FlowRuleBatchEntry entry2 =
                new FlowRuleBatchEntry(FlowRuleBatchEntry.FlowRuleOperation.ADD, rule1, 100L);

        testPerformance("FlowRuleBatchEntry1", entry1);
        testPerformance("FlowRuleBatchEntry2", entry2);
    }

    @Test
    public void testIpPrefix() {
        testPerformance("IpPrefix", IpPrefix.valueOf("192.168.0.1/24"));
    }

    @Test
    public void testIp4Prefix() {
        testPerformance("Ip4Prefix", Ip4Prefix.valueOf("192.168.0.1/24"));
    }

    @Test
    public void testIp6Prefix() {
        testPerformance("Ip6Prefix", Ip6Prefix.valueOf("1111:2222::/120"));
    }

    @Test
    public void testIpAddress() {
        testPerformance("IpAddress", IpAddress.valueOf("192.168.0.1"));
    }

    @Test
    public void testIp4Address() {
        testPerformance("Ip4Address", Ip4Address.valueOf("192.168.0.1"));
    }

    @Test
    public void testIp6Address() {
        testPerformance("Ip6Address", Ip6Address.valueOf("1111:2222::"));
    }

    @Test
    public void testMacAddress() {
        testPerformance("MacAddress", MacAddress.valueOf("12:34:56:78:90:ab"));
    }

    @Test
    public void testLinkKey() {
        testPerformance("LinkKey", LinkKey.linkKey(CP1, CP2));
    }

    @Test
    public void testNodeId() {
        testPerformance("NodeId", new NodeId("SomeNodeIdentifier"));
    }

    @Test
    public void testPortNumber() {
        testPerformance("PortNumber", P1);
    }

    @Test
    public void testProviderId() {
        testPerformance("ProviderId", PID);
    }

    @Test
    public void testMastershipTerm() {
        testPerformance("MastershipTerm", MastershipTerm.of(new NodeId("foo"), 2));
    }

    @Test
    public void testHostLocation() {
        testPerformance("HostLocation", new HostLocation(CP1, 1234L));
    }

    @Test
    public void testFlowId() {
        testPerformance("FlowId", FlowId.valueOf(0x12345678L));
    }

    @Test
    public void testRoleInfo() {
        testPerformance("RoleInfo", new RoleInfo(new NodeId("master"),
                asList(new NodeId("stby1"), new NodeId("stby2"))));
    }

    @Test
    public void testOchSignal() {
        testPerformance("Lambda", org.onosproject.net.Lambda.ochSignal(
                GridType.DWDM, ChannelSpacing.CHL_100GHZ, 1, 1
        ));
    }

    @Test
    public void testResource() {
        testPerformance("Resource", Resources.discrete(DID1, P1, VLAN1).resource());
    }

    @Test
    public void testResourceId() {
        testPerformance("ResourceId", Resources.discrete(DID1, P1).id());
    }

    @Test
    public void testResourceAllocation() {
        testPerformance("ResourceAllocation", new ResourceAllocation(
                Resources.discrete(DID1, P1, VLAN1).resource(),
                ResourceConsumerId.of(30L, IntentId.class)));
    }

    @Test
    public void testFrequency() {
        testPerformance("Frequency", Frequency.ofGHz(100));
    }

    @Test
    public void testBandwidth() {
        testPerformance("Bandwidth", Bandwidth.mbps(1000));
    }

    @Test
    public void testBandwidthConstraint() {
        testPerformance("BandwidthConstraint", new BandwidthConstraint(Bandwidth.bps(1000.0)));
    }

    @Test
    public void testLinkTypeConstraint() {
        testPerformance("LinkTypeConstraint", new LinkTypeConstraint(true, Link.Type.DIRECT));
    }

    @Test
    public void testLatencyConstraint() {
        testPerformance("LatencyConstraint", new LatencyConstraint(Duration.ofSeconds(10)));
    }

    @Test
    public void testWaypointConstraint() {
        testPerformance("WaypointConstraint", new WaypointConstraint(deviceId("of:1"), deviceId("of:2")));
    }

    @Test
    public void testObstacleConstraint() {
        testPerformance("ObstacleConstraint", new ObstacleConstraint(deviceId("of:1"), deviceId("of:2")));
    }

    @Test
    public void testArraysAsList() {
        testPerformance("Arrays.asList", Arrays.asList(1, 2, 3));
    }

    @Test
    public void testAnnotationConstraint() {
        testPerformance("AnnotationConstraint", new AnnotationConstraint("distance", 100.0));
    }

    @Test
    public void testGroupId() {
        testPerformance("GroupId", new GroupId(99));
    }

    @Test
    public void testEmptySet() {
        testPerformance("Collections", Collections.emptySet());
    }

    @Test
    public void testMarkerResource() {
        testPerformance("MarkerResource", MarkerResource.marker("testString"));
    }

    @Test
    public void testBitSet() {
        BitSet bs = new BitSet(32);
        bs.set(2);
        bs.set(8);
        bs.set(12);
        bs.set(18);
        bs.set(25);
        bs.set(511);

        testPerformance("BitSet", bs);
    }

}
