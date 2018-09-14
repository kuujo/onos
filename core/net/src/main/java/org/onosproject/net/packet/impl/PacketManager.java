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
package org.onosproject.net.packet.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onlab.packet.ARP;
import org.onlab.packet.ChassisId;
import org.onlab.packet.DHCP;
import org.onlab.packet.DHCP6;
import org.onlab.packet.Data;
import org.onlab.packet.DscpClass;
import org.onlab.packet.EAP;
import org.onlab.packet.EAPOL;
import org.onlab.packet.EthType;
import org.onlab.packet.Ethernet;
import org.onlab.packet.ICMP;
import org.onlab.packet.ICMP6;
import org.onlab.packet.ICMPEcho;
import org.onlab.packet.IGMP;
import org.onlab.packet.IGMPGroup;
import org.onlab.packet.IGMPMembership;
import org.onlab.packet.IGMPQuery;
import org.onlab.packet.IP;
import org.onlab.packet.IPv4;
import org.onlab.packet.IPv6;
import org.onlab.packet.LLC;
import org.onlab.packet.LLDP;
import org.onlab.packet.LLDPOrganizationalTLV;
import org.onlab.packet.LLDPTLV;
import org.onlab.packet.MPLS;
import org.onlab.packet.MplsLabel;
import org.onlab.packet.ONOSLLDP;
import org.onlab.packet.PIM;
import org.onlab.packet.RADIUS;
import org.onlab.packet.RADIUSAttribute;
import org.onlab.packet.RIP;
import org.onlab.packet.RIPV2AuthEntry;
import org.onlab.packet.RIPV2Entry;
import org.onlab.packet.RIPng;
import org.onlab.packet.RIPngEntry;
import org.onlab.packet.TCP;
import org.onlab.packet.TpPort;
import org.onlab.packet.UDP;
import org.onlab.packet.VXLAN;
import org.onlab.util.KryoNamespace;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.driver.Driver;
import org.onosproject.net.driver.DriverService;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.flowobjective.Objective;
import org.onosproject.net.flowobjective.ObjectiveContext;
import org.onosproject.net.flowobjective.ObjectiveError;
import org.onosproject.net.packet.DefaultInboundPacket;
import org.onosproject.net.packet.DefaultPacketContext;
import org.onosproject.net.packet.DefaultPacketRequest;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.OutboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketEvent;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketProcessorEntry;
import org.onosproject.net.packet.PacketProvider;
import org.onosproject.net.packet.PacketProviderRegistry;
import org.onosproject.net.packet.PacketProviderService;
import org.onosproject.net.packet.PacketRequest;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.packet.PacketStore;
import org.onosproject.net.packet.PacketStoreDelegate;
import org.onosproject.net.provider.AbstractProvider;
import org.onosproject.net.provider.AbstractProviderService;
import org.onosproject.net.provider.AbstractProxyProviderRegistry;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.Serializer;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.security.AppGuard.checkPermission;
import static org.onosproject.security.AppPermission.Type.PACKET_EVENT;
import static org.onosproject.security.AppPermission.Type.PACKET_READ;
import static org.onosproject.security.AppPermission.Type.PACKET_WRITE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Provides a basic implementation of the packet SB &amp; NB APIs.
 */
@Component(immediate = true)
@Service
public class PacketManager
        extends AbstractProxyProviderRegistry<PacketProvider, PacketProviderService,
                PacketManager.PacketProxy, PacketManager.PacketProxyService>
        implements PacketService, PacketProviderRegistry {

    private final Logger log = getLogger(getClass());

    private static final String ERROR_NULL_PROCESSOR = "Processor cannot be null";
    private static final String ERROR_NULL_SELECTOR = "Selector cannot be null";
    private static final String ERROR_NULL_APP_ID = "Application ID cannot be null";
    private static final String ERROR_NULL_DEVICE_ID = "Device ID cannot be null";
    private static final String SUPPORT_PACKET_REQUEST_PROPERTY = "supportPacketRequest";

    private final Serializer serializer = Serializer.using(KryoNamespace.newBuilder()
        .register(KryoNamespaces.API)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(new CorrelatedPacketContextSerializer(), CorrelatedPacketContext.class)
        .register(DefaultTrafficTreatment.Builder.class)
        .register(new DefaultInboundPacketSerializer(), DefaultInboundPacket.class)
        .register(
            ARP.class,
            ChassisId.class,
            Data.class,
            DHCP.class,
            DHCP6.class,
            DscpClass.class,
            EAP.class,
            EAPOL.class,
            Ethernet.class,
            EthType.class,
            ICMP.class,
            ICMP6.class,
            ICMPEcho.class,
            IGMP.class,
            IGMPGroup.class,
            IGMPMembership.class,
            IGMPQuery.class,
            IP.class,
            IPv4.class,
            IPv6.class,
            LLC.class,
            LLDP.class,
            LLDPOrganizationalTLV.class,
            LLDPTLV.class,
            MPLS.class,
            MplsLabel.class,
            ONOSLLDP.class,
            PIM.class,
            RADIUS.class,
            RADIUSAttribute.class,
            RIP.class,
            RIPng.class,
            RIPngEntry.class,
            RIPV2AuthEntry.class,
            RIPV2Entry.class,
            TCP.class,
            TpPort.class,
            UDP.class,
            VXLAN.class)
        .build());

    private final PacketStoreDelegate delegate = new InternalStoreDelegate();

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DriverService driverService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PacketStore store;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected MastershipService mastershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowObjectiveService objectiveService;

    private ExecutorService eventHandlingExecutor;

    private final DeviceListener deviceListener = new InternalDeviceListener();

    private final List<ProcessorEntry> processors = Lists.newCopyOnWriteArrayList();

    private final PacketDriverProvider defaultProvider = new PacketDriverProvider();

    private final AtomicLong correlationId = new AtomicLong();
    private final Map<Long, PacketContext> packetContexts = Maps.newConcurrentMap();

    private ApplicationId appId;
    private NodeId localNodeId;

    public PacketManager() {
        super(PacketProxy.class, PacketProxyService.class);
    }

    @Activate
    public void activate() {
        activateProxy();
        eventHandlingExecutor = Executors.newSingleThreadExecutor(
                groupedThreads("onos/net/packet", "event-handler", log));
        localNodeId = clusterService.getLocalNode().id();
        appId = coreService.getAppId(CoreService.CORE_APP_NAME);
        store.setDelegate(delegate);
        deviceService.addListener(deviceListener);
        defaultProvider.init(deviceService);
        store.existingRequests().forEach(request -> {
            if (request.deviceId().isPresent()) {
                Device device = deviceService.getDevice(request.deviceId().get());
                if (device != null) {
                    pushRule(device, request);
                } else {
                    log.info("Device is not ready yet. Skip processing packet requests {}", request);
                }
            } else {
                pushToAllDevices(request);
            }
        });
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        deactivateProxy();
        store.unsetDelegate(delegate);
        deviceService.removeListener(deviceListener);
        eventHandlingExecutor.shutdown();
        log.info("Stopped");
    }

    @Override
    protected PacketProvider defaultProvider() {
        return defaultProvider;
    }

    @Override
    public void addProcessor(PacketProcessor processor, int priority) {
        checkPermission(PACKET_EVENT);
        checkNotNull(processor, ERROR_NULL_PROCESSOR);
        ProcessorEntry entry = new ProcessorEntry(processor, priority);

        // Insert the new processor according to its priority.
        int i = 0;
        for (; i < processors.size(); i++) {
            if (priority < processors.get(i).priority()) {
                break;
            }
        }
        processors.add(i, entry);
    }

    @Override
    public void removeProcessor(PacketProcessor processor) {
        checkPermission(PACKET_EVENT);
        checkNotNull(processor, ERROR_NULL_PROCESSOR);

        // Remove the processor entry.
        for (int i = 0; i < processors.size(); i++) {
            if (processors.get(i).processor() == processor) {
                processors.remove(i);
                break;
            }
        }
    }

    @Override
    public List<PacketProcessorEntry> getProcessors() {
        checkPermission(PACKET_READ);
        return ImmutableList.copyOf(processors);
    }

    @Override
    public void requestPackets(TrafficSelector selector, PacketPriority priority,
                               ApplicationId appId) {
        checkPermission(PACKET_READ);
        checkNotNull(selector, ERROR_NULL_SELECTOR);
        checkNotNull(appId, ERROR_NULL_APP_ID);

        PacketRequest request = new DefaultPacketRequest(selector, priority, appId,
                                                         localNodeId, Optional.empty());
        store.requestPackets(request);
    }

    @Override
    public void requestPackets(TrafficSelector selector, PacketPriority priority,
                               ApplicationId appId, Optional<DeviceId> deviceId) {
        checkPermission(PACKET_READ);
        checkNotNull(selector, ERROR_NULL_SELECTOR);
        checkNotNull(appId, ERROR_NULL_APP_ID);
        checkNotNull(deviceId, ERROR_NULL_DEVICE_ID);

        PacketRequest request =
                new DefaultPacketRequest(selector, priority, appId,
                                         localNodeId, deviceId);

        store.requestPackets(request);

    }

    @Override
    public void cancelPackets(TrafficSelector selector, PacketPriority priority,
                              ApplicationId appId) {
        checkPermission(PACKET_READ);
        checkNotNull(selector, ERROR_NULL_SELECTOR);
        checkNotNull(appId, ERROR_NULL_APP_ID);


        PacketRequest request = new DefaultPacketRequest(selector, priority, appId,
                                                         localNodeId, Optional.empty());
        store.cancelPackets(request);
    }

    @Override
    public void cancelPackets(TrafficSelector selector, PacketPriority priority,
                              ApplicationId appId, Optional<DeviceId> deviceId) {
        checkPermission(PACKET_READ);
        checkNotNull(selector, ERROR_NULL_SELECTOR);
        checkNotNull(appId, ERROR_NULL_APP_ID);
        checkNotNull(deviceId, ERROR_NULL_DEVICE_ID);

        PacketRequest request = new DefaultPacketRequest(selector, priority,
                                                         appId, localNodeId,
                                                         deviceId);
        store.cancelPackets(request);
    }

    @Override
    public List<PacketRequest> getRequests() {
        checkPermission(PACKET_READ);
        return store.existingRequests();
    }

    /**
     * Pushes all rules to the specified device.
     *
     * @param device device on which to install packet request flows
     */
    private void pushRulesToDevice(Device device) {
        log.debug("Pushing packet requests to device {}", device.id());
        for (PacketRequest request : store.existingRequests()) {
            if (!request.deviceId().isPresent()) {
                pushRule(device, request);
            } else if (request.deviceId().get().equals(device.id())) {
                pushRule(device, request);
            }

        }
    }

    /**
     * Pushes a packet request flow rule to all devices.
     *
     * @param request the packet request
     */
    private void pushToAllDevices(PacketRequest request) {
        log.debug("Pushing packet request {} to all devices", request);
        for (Device device : deviceService.getDevices()) {
            Driver driver = driverService.getDriver(device.id());
            if (driver != null &&
                    Boolean.parseBoolean(driver.getProperty(SUPPORT_PACKET_REQUEST_PROPERTY))) {
                pushRule(device, request);
            }
        }
    }

    /**
     * Removes packet request flow rule from all devices.
     *
     * @param request the packet request
     */
    private void removeFromAllDevices(PacketRequest request) {
        deviceService.getAvailableDevices().forEach(d -> removeRule(d, request));
    }

    /**
     * Pushes packet intercept flow rules to the device.
     *
     * @param device  the device to push the rules to
     * @param request the packet request
     */
    private void pushRule(Device device, PacketRequest request) {
        if (!device.type().equals(Device.Type.SWITCH)) {
            return;
        }

        if (!deviceService.isAvailable(device.id())) {
            return;
        }

        ForwardingObjective forwarding = createBuilder(request)
                .add(new ObjectiveContext() {
                    @Override
                    public void onError(Objective objective, ObjectiveError error) {
                        log.warn("Failed to install packet request {} to {}: {}",
                                 request, device.id(), error);
                    }
                });

        objectiveService.forward(device.id(), forwarding);
    }

    /**
     * Removes packet intercept flow rules from the device.
     *
     * @param device  the device to remove the rules deom
     * @param request the packet request
     */
    private void removeRule(Device device, PacketRequest request) {
        if (!device.type().equals(Device.Type.SWITCH)) {
            return;
        }
        ForwardingObjective forwarding = createBuilder(request)
                .remove(new ObjectiveContext() {
                    @Override
                    public void onError(Objective objective, ObjectiveError error) {
                        log.warn("Failed to withdraw packet request {} from {}: {}",
                                 request, device.id(), error);
                    }
                });
        objectiveService.forward(device.id(), forwarding);
    }

    private DefaultForwardingObjective.Builder createBuilder(PacketRequest request) {
        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .punt()
                .wipeDeferred()
                .build();

        return DefaultForwardingObjective.builder()
                .withPriority(request.priority().priorityValue())
                .withSelector(request.selector())
                .fromApp(appId)
                .withFlag(ForwardingObjective.Flag.VERSATILE)
                .withTreatment(treatment)
                .makePermanent();
    }

    @Override
    public void emit(OutboundPacket packet) {
        checkPermission(PACKET_WRITE);
        checkNotNull(packet, "Packet cannot be null");
        store.emit(packet);
    }

    private void localEmit(OutboundPacket packet) {
        Device device = deviceService.getDevice(packet.sendThrough());
        if (device == null) {
            return;
        }
        PacketProvider packetProvider = getProvider(device.providerId());
        if (packetProvider != null) {
            packetProvider.emit(packet);
        }
    }

    @Override
    protected Serializer getProxySerializer() {
        return serializer;
    }

    @Override
    protected PacketProviderService createProxyProviderService(PacketProvider provider) {
        return new ProxyPacketProviderService(provider);
    }

    @Override
    protected PacketProviderService createControllerProviderService(PacketProvider provider) {
        return new ControllerPacketProviderService(provider);
    }

    @Override
    protected PacketProvider createControllerProvider() {
        return new ControllerPacketProvider();
    }

    @Override
    protected PacketProxy createProxy() {
        return new ProxyPacketProxy();
    }

    @Override
    protected PacketProxyService createProxyService() {
        return new ControllerPacketProxyService();
    }

    /**
     * Personalized packet provider service issued to the supplied provider.
     */
    private class ControllerPacketProviderService
            extends AbstractProviderService<PacketProvider>
            implements PacketProviderService {

        protected ControllerPacketProviderService(PacketProvider provider) {
            super(provider);
        }

        @Override
        public void processPacket(PacketContext context) {
            // TODO filter packets sent to processors based on registrations
            for (ProcessorEntry entry : processors) {
                try {
                    if (log.isTraceEnabled()) {
                        log.trace("Starting packet processing by {}",
                                entry.processor().getClass().getName());
                    }

                    long start = System.nanoTime();
                    entry.processor().process(context);
                    entry.addNanos(System.nanoTime() - start);

                    if (log.isTraceEnabled()) {
                        log.trace("Finished packet processing by {}",
                                entry.processor().getClass().getName());
                    }
                } catch (Exception e) {
                    log.warn("Packet processor {} threw an exception", entry.processor(), e);
                }
            }
        }

    }


    /**
     * Internal callback from the packet store.
     */
    protected class InternalStoreDelegate implements PacketStoreDelegate {
        @Override
        public void notify(PacketEvent event) {
            localEmit(event.subject());
        }

        @Override
        public void requestPackets(PacketRequest request) {
            DeviceId deviceid = request.deviceId().orElse(null);

            if (deviceid != null) {
                Device device = deviceService.getDevice(deviceid);

                if (device != null) {
                    pushRule(deviceService.getDevice(deviceid), request);
                }
            } else {
                pushToAllDevices(request);
            }
        }

        @Override
        public void cancelPackets(PacketRequest request) {
            DeviceId deviceid = request.deviceId().orElse(null);

            if (deviceid != null) {
                Device device = deviceService.getDevice(deviceid);

                if (device != null) {
                    removeRule(deviceService.getDevice(deviceid), request);
                }
            } else {
                removeFromAllDevices(request);
            }
        }
    }

    /**
     * Internal listener for device service events.
     */
    private class InternalDeviceListener implements DeviceListener {

        @Override
        public boolean isRelevant(DeviceEvent event) {
            return event.type() == DeviceEvent.Type.DEVICE_ADDED ||
                    event.type() == DeviceEvent.Type.DEVICE_AVAILABILITY_CHANGED;
        }

        @Override
        public void event(DeviceEvent event) {
            eventHandlingExecutor.execute(() -> {
                try {
                    if (driverService == null) {
                        // Event came in after the driver service shut down, nothing to be done
                        return;
                    }
                    Device device = event.subject();
                    Driver driver = driverService.getDriver(device.id());
                    if (driver == null) {
                        return;
                    }
                    if (!Boolean.parseBoolean(driver.getProperty(SUPPORT_PACKET_REQUEST_PROPERTY))) {
                        return;
                    }
                    if (!deviceService.isAvailable(event.subject().id())) {
                        return;
                    }
                    pushRulesToDevice(device);
                } catch (Exception e) {
                    log.warn("Failed to process {}", event, e);
                }
            });
        }
    }

    /**
     * Entity for tracking stats for a packet processor.
     */
    private class ProcessorEntry implements PacketProcessorEntry {
        private final PacketProcessor processor;
        private final int priority;
        private long invocations = 0;
        private long nanos = 0;

        public ProcessorEntry(PacketProcessor processor, int priority) {
            this.processor = processor;
            this.priority = priority;
        }

        @Override
        public PacketProcessor processor() {
            return processor;
        }

        @Override
        public int priority() {
            return priority;
        }

        @Override
        public long invocations() {
            return invocations;
        }

        @Override
        public long totalNanos() {
            return nanos;
        }

        @Override
        public long averageNanos() {
            return invocations > 0 ? nanos / invocations : 0;
        }

        void addNanos(long nanos) {
            this.nanos += nanos;
            this.invocations++;
        }
    }

    interface PacketProxy {
        void emit(OutboundPacket packet);
        void send(long id, PacketContext outboundContext);
    }

    interface PacketProxyService {
        void processPacket(ProviderId providerId, PacketContext context);
    }

    private class ControllerPacketProxyService implements PacketProxyService {
        @Override
        public void processPacket(ProviderId providerId, PacketContext context) {
            createProviderService(getProvider(providerId)).processPacket(context);
        }
    }

    private class ControllerPacketProvider extends AbstractProvider implements PacketProvider {
        ControllerPacketProvider() {
            super(ProviderId.NONE);
        }

        @Override
        public void emit(OutboundPacket packet) {
            proxyFactory.getProxyFor(proxyRoleService.getProxyNode()).emit(packet);
        }
    }

    private class ProxyPacketProviderService extends AbstractProviderService<PacketProvider> implements PacketProviderService {
        ProxyPacketProviderService(PacketProvider provider) {
            super(provider);
        }

        @Override
        public void processPacket(PacketContext context) {
            Device device = deviceService.getDevice(context.outPacket().sendThrough());
            if (device == null) {
                return;
            }

            NodeId master = mastershipService.getMasterFor(device.id());
            if (master != null) {
                long id = correlationId.incrementAndGet();
                if (context.outPacket() != null) {
                    packetContexts.put(id, context);
                }
                PacketContext correlatedContext = new CorrelatedPacketContext(
                    context.time(), context.inPacket(), context.outPacket(), context.block(), id);
                proxyServiceFactory.getProxyFor(master)
                    .processPacket(provider().id(), correlatedContext);
            } else {
                log.warn("Failed processPacket by proxy: no master found for device {}", device.id());
            }
        }
    }

    private class ProxyPacketProxy implements PacketProxy {
        @Override
        public void emit(OutboundPacket packet) {
            localEmit(packet);
        }

        @Override
        public void send(long id, PacketContext outboundContext) {
            PacketContext context = packetContexts.remove(id);
            if (context != null) {
                context.outPacket().data().put(outboundContext.outPacket().data());
                context.treatmentBuilder().addTreatment(outboundContext.treatmentBuilder().build());
                context.send();
            }
        }
    }

    private class CorrelatedPacketContext extends DefaultPacketContext {
        private final long correlationId;

        CorrelatedPacketContext(long time, InboundPacket inPkt, OutboundPacket outPkt, boolean block, long correlationId) {
            super(time, inPkt, outPkt, block);
            this.correlationId = correlationId;
        }

        @Override
        public void send() {
            proxyFactory.getProxyFor(proxyRoleService.getProxyNode()).send(correlationId, this);
        }
    }

    private static class DefaultInboundPacketSerializer extends com.esotericsoftware.kryo.Serializer<DefaultInboundPacket> {
        @Override
        public void write(Kryo kryo, Output output, DefaultInboundPacket packet) {
            kryo.writeObject(output, packet.receivedFrom().toString());
            kryo.writeObject(output, packet.parsed());
            kryo.writeObject(output, packet.unparsed().array());
            kryo.writeObjectOrNull(output, packet.cookie().orElse(null), Long.class);
        }

        @Override
        public DefaultInboundPacket read(Kryo kryo, Input input, Class<DefaultInboundPacket> type) {
            ConnectPoint receivedFrom = ConnectPoint.fromString(kryo.readObject(input, String.class));
            Ethernet parsed = kryo.readObject(input, Ethernet.class);
            ByteBuffer unparsed = ByteBuffer.wrap(kryo.readObject(input, byte[].class));
            Optional<Long> cookie = Optional.ofNullable(kryo.readObjectOrNull(input, Long.class));
            return new DefaultInboundPacket(receivedFrom, parsed, unparsed, cookie);
        }
    }

    private class CorrelatedPacketContextSerializer extends com.esotericsoftware.kryo.Serializer<CorrelatedPacketContext> {
        @Override
        public void write(Kryo kryo, Output output, CorrelatedPacketContext context) {
            kryo.writeObject(output, context.correlationId);
            kryo.writeObject(output, context.time());
            kryo.writeClassAndObject(output, context.inPacket());
            kryo.writeClassAndObject(output, context.outPacket());
            kryo.writeObject(output, context.block());
        }

        @Override
        public CorrelatedPacketContext read(Kryo kryo, Input input, Class<CorrelatedPacketContext> type) {
            long correlationId = kryo.readObject(input, Long.TYPE);
            long time = kryo.readObject(input, Long.TYPE);
            InboundPacket inboundPacket = (InboundPacket) kryo.readClassAndObject(input);
            OutboundPacket outboundPacket = (OutboundPacket) kryo.readClassAndObject(input);
            boolean block = kryo.readObject(input, Boolean.TYPE);
            return new CorrelatedPacketContext(time, inboundPacket, outboundPacket, block, correlationId);
        }
    }
}
