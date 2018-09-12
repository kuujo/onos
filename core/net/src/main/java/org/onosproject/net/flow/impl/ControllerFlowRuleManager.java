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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.ProxyEgressService;
import org.onosproject.cluster.ProxyFactory;
import org.onosproject.cluster.ProxyService;
import org.onosproject.core.ApplicationId;
import org.onosproject.net.DeviceId;
import org.onosproject.net.device.DeviceProxyService;
import org.onosproject.net.flow.CompletedBatchOperation;
import org.onosproject.net.flow.FlowEntry;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleProvider;
import org.onosproject.net.flow.FlowRuleProxy;
import org.onosproject.net.flow.FlowRuleProxyService;
import org.onosproject.net.flow.TableStatisticsEntry;
import org.onosproject.net.flow.oldbatch.FlowRuleBatchOperation;
import org.onosproject.net.provider.AbstractProvider;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.Serializer;
import org.osgi.service.component.ComponentContext;

/**
 * Controller level flow rule manager.
 */
@Component(immediate = true)
@Service
public class ControllerFlowRuleManager extends FlowRuleManager {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.API);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ProxyService proxyService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ProxyEgressService proxyEgressService;

    private ProxyFactory<FlowRuleProxy> proxyFactory;

    private final FlowRuleProxyService flowRuleProxyService = new InternalFlowRuleProxyService();
    private final FlowRuleProvider proxyProvider = new InternalFlowRuleProxyProvider();

    @Activate
    public void activate(ComponentContext context) {
        proxyService.registerProxyService(FlowRuleProxyService.class, flowRuleProxyService, SERIALIZER);
        proxyFactory = proxyService.getProxyFactory(FlowRuleProxy.class, SERIALIZER);
        super.activate(context);
    }

    @Deactivate
    public void deactivate() {
        proxyService.unregisterProxyService(DeviceProxyService.class);
        super.deactivate();
    }

    private class InternalFlowRuleProxyService implements FlowRuleProxyService {
        @Override
        public void flowRemoved(ProviderId providerId, FlowEntry flowEntry) {
            createProviderService(getProvider(flowEntry.deviceId())).flowRemoved(flowEntry);
        }

        @Override
        public void pushFlowMetrics(ProviderId providerId, DeviceId deviceId, Iterable<FlowEntry> flowEntries) {
            createProviderService(getProvider(deviceId)).pushFlowMetrics(deviceId, flowEntries);
        }

        @Override
        public void pushFlowMetricsWithoutFlowMissing(ProviderId providerId, DeviceId deviceId, Iterable<FlowEntry> flowEntries) {
            createProviderService(getProvider(deviceId)).pushFlowMetricsWithoutFlowMissing(deviceId, flowEntries);
        }

        @Override
        public void pushTableStatistics(ProviderId providerId, DeviceId deviceId, List<TableStatisticsEntry> tableStatsEntries) {
            createProviderService(getProvider(deviceId)).pushTableStatistics(deviceId, tableStatsEntries);
        }

        @Override
        public void batchOperationCompleted(ProviderId providerId, long batchId, CompletedBatchOperation operation) {
            createProviderService(getProvider(operation.deviceId())).batchOperationCompleted(batchId, operation);
        }
    }

    private class InternalFlowRuleProxyProvider extends AbstractProvider implements FlowRuleProvider {
        InternalFlowRuleProxyProvider() {
            super(ProviderId.NONE);
        }

        @Override
        public void applyFlowRule(FlowRule... flowRules) {
            proxyFactory.getProxyFor(proxyEgressService.getProxyNode()).applyFlowRule(flowRules);
        }

        @Override
        public void removeFlowRule(FlowRule... flowRules) {
            proxyFactory.getProxyFor(proxyEgressService.getProxyNode()).removeFlowRule(flowRules);
        }

        @Override
        public void removeRulesById(ApplicationId id, FlowRule... flowRules) {
            proxyFactory.getProxyFor(proxyEgressService.getProxyNode()).removeRulesById(id, flowRules);
        }

        @Override
        public void executeBatch(FlowRuleBatchOperation batch) {
            proxyFactory.getProxyFor(proxyEgressService.getProxyNode()).executeBatch(batch);
        }
    }
}
