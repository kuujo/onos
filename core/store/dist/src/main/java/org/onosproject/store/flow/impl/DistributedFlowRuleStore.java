 /*
 * Copyright 2014-present Open Networking Laboratory
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

 import java.util.Collections;
 import java.util.HashMap;
 import java.util.List;
 import java.util.Map;
 import java.util.Objects;
 import java.util.Set;
 import java.util.concurrent.TimeUnit;
 import java.util.stream.Collectors;

 import com.google.common.collect.ImmutableList;
 import com.google.common.collect.Iterables;
 import com.google.common.collect.Maps;
 import com.google.common.collect.Streams;
 import org.apache.felix.scr.annotations.Activate;
 import org.apache.felix.scr.annotations.Component;
 import org.apache.felix.scr.annotations.Deactivate;
 import org.apache.felix.scr.annotations.Reference;
 import org.apache.felix.scr.annotations.ReferenceCardinality;
 import org.apache.felix.scr.annotations.Service;
 import org.onlab.util.KryoNamespace;
 import org.onosproject.core.CoreService;
 import org.onosproject.core.IdGenerator;
 import org.onosproject.net.DeviceId;
 import org.onosproject.net.device.DeviceService;
 import org.onosproject.net.flow.CompletedBatchOperation;
 import org.onosproject.net.flow.DefaultFlowEntry;
 import org.onosproject.net.flow.FlowEntry;
 import org.onosproject.net.flow.FlowEntry.FlowEntryState;
 import org.onosproject.net.flow.FlowId;
 import org.onosproject.net.flow.FlowRule;
 import org.onosproject.net.flow.FlowRuleBatchEntry;
 import org.onosproject.net.flow.FlowRuleBatchEntry.FlowRuleOperation;
 import org.onosproject.net.flow.FlowRuleBatchEvent;
 import org.onosproject.net.flow.FlowRuleBatchOperation;
 import org.onosproject.net.flow.FlowRuleBatchRequest;
 import org.onosproject.net.flow.FlowRuleEvent;
 import org.onosproject.net.flow.FlowRuleEvent.Type;
 import org.onosproject.net.flow.FlowRuleService;
 import org.onosproject.net.flow.FlowRuleStore;
 import org.onosproject.net.flow.FlowRuleStoreDelegate;
 import org.onosproject.net.flow.StoredFlowEntry;
 import org.onosproject.net.flow.TableStatisticsEntry;
 import org.onosproject.store.AbstractStore;
 import org.onosproject.store.impl.MastershipBasedTimestamp;
 import org.onosproject.store.serializers.KryoNamespaces;
 import org.onosproject.store.service.DocumentPath;
 import org.onosproject.store.service.DocumentTree;
 import org.onosproject.store.service.EventuallyConsistentMap;
 import org.onosproject.store.service.EventuallyConsistentMapEvent;
 import org.onosproject.store.service.EventuallyConsistentMapListener;
 import org.onosproject.store.service.NoSuchDocumentPathException;
 import org.onosproject.store.service.Serializer;
 import org.onosproject.store.service.StorageService;
 import org.onosproject.store.service.Versioned;
 import org.onosproject.store.service.WallClockTimestamp;
 import org.osgi.service.component.ComponentContext;
 import org.slf4j.Logger;

 import static org.onosproject.net.flow.FlowRuleEvent.Type.RULE_REMOVED;
 import static org.onosproject.net.flow.FlowRuleEvent.Type.RULE_UPDATED;
 import static org.slf4j.LoggerFactory.getLogger;

/**
 * Manages inventory of flow rules using a distributed state management protocol.
 */
@Component(immediate = true)
@Service
public class DistributedFlowRuleStore
        extends AbstractStore<FlowRuleBatchEvent, FlowRuleStoreDelegate>
        implements FlowRuleStore {

    private final Logger log = getLogger(getClass());

    private static final String FLOW_TABLE = "onos-flow-table";

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    private EventuallyConsistentMap<DeviceId, List<TableStatisticsEntry>> deviceTableStats;
    private final EventuallyConsistentMapListener<DeviceId, List<TableStatisticsEntry>> tableStatsListener =
            new InternalTableStatsListener();

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    protected final Serializer serializer = Serializer.using(KryoNamespaces.API);

    protected final KryoNamespace.Builder serializerBuilder = KryoNamespace.newBuilder()
            .register(KryoNamespaces.API)
            .register(MastershipBasedTimestamp.class);

    private DocumentTree<Object> flows;
    private IdGenerator idGenerator;

    @Activate
    public void activate(ComponentContext context) {
        idGenerator = coreService.getIdGenerator(FlowRuleService.FLOW_OP_TOPIC);

        deviceTableStats = storageService.<DeviceId, List<TableStatisticsEntry>>eventuallyConsistentMapBuilder()
                .withName("onos-flow-table-stats")
                .withSerializer(serializerBuilder)
                .withAntiEntropyPeriod(5, TimeUnit.SECONDS)
                .withTimestampProvider((k, v) -> new WallClockTimestamp())
                .withTombstonesDisabled()
                .build();
        deviceTableStats.addListener(tableStatsListener);

        flows = storageService.documentTreeBuilder()
                .withName(FLOW_TABLE)
                .withSerializer(serializer)
                .buildDocumentTree()
                .asDocumentTree();

        log.info("Started");
    }

    @Deactivate
    public void deactivate(ComponentContext context) {
        deviceTableStats.removeListener(tableStatsListener);
        deviceTableStats.destroy();
        log.info("Stopped");
    }

    // This is not a efficient operation on a distributed sharded
    // flow store. We need to revisit the need for this operation or at least
    // make it device specific.
    @Override
    public int getFlowRuleCount() {
        return Streams.stream(deviceService.getDevices()).parallel()
                        .mapToInt(device -> Iterables.size(getFlowEntries(device.id())))
                        .sum();
    }

    private DocumentPath getPathFor(DeviceId deviceId) {
        return DocumentPath.from(FLOW_TABLE, deviceId.toString());
    }

    private DocumentPath getPathFor(DeviceId deviceId, FlowId flowId) {
        return DocumentPath.from(FLOW_TABLE, deviceId.toString(), flowId.toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public FlowEntry getFlowEntry(FlowRule rule) {
        DocumentPath path = getPathFor(rule.deviceId(), rule.id());
        return ((Map<StoredFlowEntry, StoredFlowEntry>) flows.get(path).value()).get(rule);
    }

    @Override
    public Iterable<FlowEntry> getFlowEntries(DeviceId deviceId) {
        DocumentPath path = getPathFor(deviceId);
        try {
            return getFlowEntries(path);
        } catch (NoSuchDocumentPathException e) {
            flows.createRecursive(path, null);
            return getFlowEntries(path);
        }
    }

    @SuppressWarnings("unchecked")
    private Iterable<FlowEntry> getFlowEntries(DocumentPath path) {
        return flows.getChildren(path)
                .values()
                .stream()
                .flatMap(v -> ((Map<FlowEntry, FlowEntry>) v.value()).values().stream())
                .collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void storeFlowRule(FlowRule rule) {
        DocumentPath path = getPathFor(rule.deviceId(), rule.id());
        flows.createRecursive(path, new HashMap<>());
        while (true) {
            Versioned<Object> value = flows.get(path);
            if (value != null) {
                Map<StoredFlowEntry, StoredFlowEntry> entries =
                        Maps.newHashMap((Map<StoredFlowEntry, StoredFlowEntry>) value.value());
                StoredFlowEntry entry = new DefaultFlowEntry(rule);
                entries.put(entry, entry);
                if (flows.replace(path, entries, value.version())) {
                    log.info("Stored new flow rule");
                    return;
                } else {
                    log.warn("Failed to store new flow rule");
                }
            }
        }
    }

    @Override
    public void storeBatch(FlowRuleBatchOperation operation) {
        if (operation.getOperations().isEmpty()) {
            notifyDelegate(FlowRuleBatchEvent.completed(
                    new FlowRuleBatchRequest(operation.id(), Collections.emptySet()),
                    new CompletedBatchOperation(true, Collections.emptySet(), operation.deviceId())));
        } else {
            storeBatchInternal(operation);
        }
    }

    private void storeBatchInternal(FlowRuleBatchOperation operation) {
        final DeviceId did = operation.deviceId();
        //final Collection<FlowEntry> ft = flowTable.getFlowEntries(did);
        Set<FlowRuleBatchEntry> currentOps = updateStoreInternal(operation);
        if (currentOps.isEmpty()) {
            batchOperationComplete(FlowRuleBatchEvent.completed(
                    new FlowRuleBatchRequest(operation.id(), Collections.emptySet()),
                    new CompletedBatchOperation(true, Collections.emptySet(), did)));
            return;
        }

        notifyDelegate(FlowRuleBatchEvent.requested(new
                           FlowRuleBatchRequest(operation.id(),
                                                currentOps), operation.deviceId()));
    }

    private Set<FlowRuleBatchEntry> updateStoreInternal(FlowRuleBatchOperation operation) {
        return operation.getOperations().stream().map(
                op -> {
                    switch (op.operator()) {
                        case ADD:
                            addBatchEntry(op);
                            return op;
                        case REMOVE:
                            removeBatchEntry(op);
                            break;
                        case MODIFY:
                            //TODO: figure this out at some point
                            break;
                        default:
                            log.warn("Unknown flow operation operator: {}", op.operator());
                    }
                    return null;
                }
        ).filter(Objects::nonNull).collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private void addBatchEntry(FlowRuleBatchEntry batchEntry) {
        StoredFlowEntry entry = new DefaultFlowEntry(batchEntry.target());
        DocumentPath path = getPathFor(entry.deviceId(), entry.id());
        flows.createRecursive(path, new HashMap<>());
        while (true) {
            Versioned<Object> value = flows.get(path);
            if (value != null) {
                Map<StoredFlowEntry, StoredFlowEntry> entries =
                        Maps.newHashMap((Map<StoredFlowEntry, StoredFlowEntry>) value.value());
                entries.put(entry, entry);
                if (flows.replace(path, entries, value.version())) {
                    log.info("Stored new flow rule");
                    return;
                } else {
                    log.warn("Failed to store new flow rule");
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void removeBatchEntry(FlowRuleBatchEntry batchEntry) {
        updateFlowRuleState(batchEntry.target(), FlowEntryState.PENDING_REMOVE);
    }

    @SuppressWarnings("unchecked")
    private boolean updateFlowRuleState(FlowRule rule, FlowEntryState state) {
        DocumentPath path = getPathFor(rule.deviceId(), rule.id());
        while (true) {
            Versioned<Object> value = flows.get(path);
            if (value != null) {
                Map<StoredFlowEntry, StoredFlowEntry> entries =
                        Maps.newHashMap((Map<StoredFlowEntry, StoredFlowEntry>) value.value());
                StoredFlowEntry entry = entries.get(rule);
                if (entry != null && entry.state() != state) {
                    entry.setState(state);
                    if (flows.replace(path, entries, value.version())) {
                        log.info("Updated flow rule state");
                        return true;
                    } else {
                        log.warn("Failed to update flow rule");
                    }
                } else {
                    return false;
                }
            }
        }
    }

    @Override
    public void deleteFlowRule(FlowRule rule) {
        storeBatch(
                new FlowRuleBatchOperation(
                        Collections.singletonList(
                                new FlowRuleBatchEntry(
                                        FlowRuleOperation.REMOVE,
                                        rule)), rule.deviceId(), idGenerator.getNewId()));
    }

    @Override
    public FlowRuleEvent pendingFlowRule(FlowEntry rule) {
        if (updateFlowRuleState(rule, FlowEntryState.PENDING_ADD)) {
            return new FlowRuleEvent(RULE_UPDATED, rule);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public FlowRuleEvent addOrUpdateFlowRule(FlowEntry rule) {
        FlowRuleEvent event = null;
        DocumentPath path = getPathFor(rule.deviceId(), rule.id());
        flows.createRecursive(path, new HashMap<>());
        while (true) {
            Versioned<Object> value = flows.get(path);
            if (value != null) {
                Map<StoredFlowEntry, StoredFlowEntry> entries =
                        Maps.newHashMap((Map<StoredFlowEntry, StoredFlowEntry>) value.value());
                StoredFlowEntry entry = entries.get(rule);
                if (entry != null) {
                    entry.setBytes(rule.bytes());
                    entry.setLife(rule.life(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
                    entry.setLiveType(rule.liveType());
                    entry.setPackets(rule.packets());
                    entry.setLastSeen();
                    if (entry.state() == FlowEntryState.PENDING_ADD) {
                        entry.setState(FlowEntryState.ADDED);
                        event = new FlowRuleEvent(Type.RULE_ADDED, rule);
                    } else {
                        event = new FlowRuleEvent(Type.RULE_UPDATED, rule);
                    }
                } else {
                    entry = new DefaultFlowEntry(rule);
                    entries.put(entry, entry);
                }

                if (flows.replace(path, entries, value.version())) {
                    log.info("Stored new flow rule");
                    return event;
                } else {
                    log.warn("Failed to store new flow rule");
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public FlowRuleEvent removeFlowRule(FlowEntry rule) {
        DocumentPath path = getPathFor(rule.deviceId(), rule.id());
        while (true) {
            Versioned<Object> value = flows.get(path);
            if (value != null) {
                Map<StoredFlowEntry, StoredFlowEntry> entries =
                        Maps.newHashMap((Map<StoredFlowEntry, StoredFlowEntry>) value.value());
                StoredFlowEntry entry = entries.remove(rule);
                if (entry != null) {
                    if (flows.replace(path, entries, value.version())) {
                        log.info("Removed flow rule");
                        return new FlowRuleEvent(RULE_REMOVED, entry);
                    } else {
                        log.warn("Failed to remove flow rule");
                    }
                }
            }
        }
    }

    @Override
    public void purgeFlowRule(DeviceId deviceId) {
        flows.removeNode(getPathFor(deviceId));
    }

    @Override
    public void purgeFlowRules() {
        for (String deviceId : flows.getChildren(flows.root()).keySet()) {
            flows.removeNode(getPathFor(DeviceId.deviceId(deviceId)));
        }
    }

    @Override
    public void batchOperationComplete(FlowRuleBatchEvent event) {
        notifyDelegate(event);
    }

    @Override
    public FlowRuleEvent updateTableStatistics(DeviceId deviceId,
                                               List<TableStatisticsEntry> tableStats) {
        deviceTableStats.put(deviceId, tableStats);
        return null;
    }

    @Override
    public Iterable<TableStatisticsEntry> getTableStatistics(DeviceId deviceId) {
        List<TableStatisticsEntry> tableStats = deviceTableStats.get(deviceId);
        if (tableStats == null) {
            return Collections.emptyList();
        }
        return ImmutableList.copyOf(tableStats);
    }

    @Override
    public long getActiveFlowRuleCount(DeviceId deviceId) {
        return Streams.stream(getTableStatistics(deviceId))
                .mapToLong(TableStatisticsEntry::activeFlowEntries)
                .sum();
    }

    private class InternalTableStatsListener
        implements EventuallyConsistentMapListener<DeviceId, List<TableStatisticsEntry>> {
        @Override
        public void event(EventuallyConsistentMapEvent<DeviceId,
                          List<TableStatisticsEntry>> event) {
            //TODO: Generate an event to listeners (do we need?)
        }
    }
}
