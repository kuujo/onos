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
package org.onosproject.cluster.impl;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.Node;
import org.onosproject.cluster.NodeId;
import org.onosproject.cluster.ProxyIngressService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxy ingress manager.
 */
@Component(immediate = true)
@Service
public class ProxyIngressManager implements ProxyIngressService {

    private static final boolean PROXY_ENABLED = Boolean.parseBoolean(System.getProperty("onos.proxy.enabled", "false"));

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterService clusterService;

    private NodeId localNodeId;
    private boolean isProxyNode;

    @Activate
    public void activate() {
        localNodeId = clusterService.getLocalNode().id();
        isProxyNode = clusterService.getProxyNodes().stream()
            .anyMatch(node -> node.id().equals(localNodeId));
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        log.info("Stopped");
    }

    @Override
    public boolean isProxyEnabled() {
        return PROXY_ENABLED;
    }

    @Override
    public boolean isProxyNode() {
        return isProxyNode;
    }

    @Override
    public Set<NodeId> getControllerNodes() {
        if (!isProxyNode) {
            return ImmutableSet.of();
        }

        // TODO: This computation should be done each time a node joins/leaves the cluster.
        Set<Node> proxyNodes = clusterService.getProxyNodes();

        List<NodeId> proxyNodeIds = proxyNodes.stream()
            .map(Node::id)
            .sorted(Comparator.comparing(NodeId::id))
            .collect(Collectors.toList());

        // Perform a reverse mapping of controller nodes to this proxy node.
        return clusterService.getNodes()
            .stream()
            .map(ControllerNode::id)
            .filter(id -> proxyNodeIds.get(Math.abs(proxyNodeIds.size() % localNodeId.id().hashCode())).equals(localNodeId))
            .collect(Collectors.toSet());
    }
}
