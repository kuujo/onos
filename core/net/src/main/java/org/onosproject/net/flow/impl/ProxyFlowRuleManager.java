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
package org.onosproject.net.flow.impl;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.ProxyFactory;
import org.onosproject.cluster.ProxyService;
import org.onosproject.core.ApplicationId;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.DeviceId;
import org.onosproject.net.device.DeviceProxy;
import org.onosproject.net.flow.CompletedBatchOperation;
import org.onosproject.net.flow.FlowEntry;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleEvent;
import org.onosproject.net.flow.FlowRuleListener;
import org.onosproject.net.flow.FlowRuleOperations;
import org.onosproject.net.flow.FlowRuleProvider;
import org.onosproject.net.flow.FlowRuleProviderRegistry;
import org.onosproject.net.flow.FlowRuleProviderService;
import org.onosproject.net.flow.FlowRuleProxy;
import org.onosproject.net.flow.FlowRuleProxyService;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TableStatisticsEntry;
import org.onosproject.net.flow.oldbatch.FlowRuleBatchOperation;
import org.onosproject.net.provider.AbstractListenerProviderRegistry;
import org.onosproject.net.provider.AbstractProviderService;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxy level flow rule manager.
 */
@Component(immediate = true)
@Service
public class ProxyFlowRuleManager
    extends AbstractListenerProviderRegistry<FlowRuleEvent, FlowRuleListener, FlowRuleProvider, FlowRuleProviderService>
    implements FlowRuleService, FlowRuleProviderRegistry {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.API);

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ProxyService proxyService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected MastershipService mastershipService;

    private ProxyFactory<FlowRuleProxyService> proxyFactory;

    private final FlowRuleProxy flowRuleProxy = new InternalFlowRuleProxy();

    @Activate
    public void activate() {
        proxyService.registerProxyService(FlowRuleProxy.class, flowRuleProxy, SERIALIZER);
        proxyFactory = proxyService.getProxyFactory(FlowRuleProxyService.class, SERIALIZER);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        proxyService.unregisterProxyService(DeviceProxy.class);
        log.info("Stopped");
    }

    @Override
    public int getFlowRuleCount() {
        return 0;
    }

    @Override
    public Iterable<FlowEntry> getFlowEntries(DeviceId deviceId) {
        return ImmutableList.of();
    }

    @Override
    public void applyFlowRules(FlowRule... flowRules) {

    }

    @Override
    public void purgeFlowRules(DeviceId deviceId) {

    }

    @Override
    public void removeFlowRules(FlowRule... flowRules) {

    }

    @Override
    public void removeFlowRulesById(ApplicationId appId) {

    }

    @Override
    public Iterable<FlowRule> getFlowRulesById(ApplicationId id) {
        return ImmutableList.of();
    }

    @Override
    public Iterable<FlowEntry> getFlowEntriesById(ApplicationId id) {
        return ImmutableList.of();
    }

    @Override
    public Iterable<FlowRule> getFlowRulesByGroupId(ApplicationId appId, short groupId) {
        return ImmutableList.of();
    }

    @Override
    public void apply(FlowRuleOperations ops) {

    }

    @Override
    public Iterable<TableStatisticsEntry> getFlowTableStatistics(DeviceId deviceId) {
        return ImmutableList.of();
    }

    @Override
    protected FlowRuleProviderService createProviderService(FlowRuleProvider provider) {
        return null;
    }

    private class ProxyFlowRuleProviderService extends AbstractProviderService<FlowRuleProvider> implements FlowRuleProviderService {
        ProxyFlowRuleProviderService(FlowRuleProvider provider) {
            super(provider);
        }

        @Override
        public void flowRemoved(FlowEntry flowEntry) {
            proxyFactory.getProxyFor(mastershipService.getMasterFor(flowEntry.deviceId()))
                .flowRemoved(provider().id(), flowEntry);
        }

        @Override
        public void pushFlowMetrics(DeviceId deviceId, Iterable<FlowEntry> flowEntries) {
            proxyFactory.getProxyFor(mastershipService.getMasterFor(deviceId))
                .pushFlowMetrics(provider().id(), deviceId, flowEntries);
        }

        @Override
        public void pushFlowMetricsWithoutFlowMissing(DeviceId deviceId, Iterable<FlowEntry> flowEntries) {
            proxyFactory.getProxyFor(mastershipService.getMasterFor(deviceId))
                .pushFlowMetricsWithoutFlowMissing(provider().id(), deviceId, flowEntries);
        }

        @Override
        public void pushTableStatistics(DeviceId deviceId, List<TableStatisticsEntry> tableStatsEntries) {
            proxyFactory.getProxyFor(mastershipService.getMasterFor(deviceId))
                .pushTableStatistics(provider().id(), deviceId, tableStatsEntries);
        }

        @Override
        public void batchOperationCompleted(long batchId, CompletedBatchOperation operation) {
            proxyFactory.getProxyFor(mastershipService.getMasterFor(operation.deviceId()))
                .batchOperationCompleted(provider().id(), batchId, operation);
        }
    }

    private class InternalFlowRuleProxy implements FlowRuleProxy {
        @Override
        public void applyFlowRule(FlowRule... flowRules) {
            for (FlowRule flowRule : flowRules) {
                getProvider(flowRule.deviceId()).applyFlowRule(flowRule);
            }
        }

        @Override
        public void removeFlowRule(FlowRule... flowRules) {
            for (FlowRule flowRule : flowRules) {
                getProvider(flowRule.deviceId()).removeFlowRule(flowRule);
            }
        }

        @Override
        public void removeRulesById(ApplicationId id, FlowRule... flowRules) {
            for (FlowRule flowRule : flowRules) {
                getProvider(flowRule.deviceId()).removeRulesById(id, flowRule);
            }
        }

        @Override
        public void executeBatch(FlowRuleBatchOperation batch) {
            getProvider(batch.deviceId()).executeBatch(batch);
        }
    }
}
