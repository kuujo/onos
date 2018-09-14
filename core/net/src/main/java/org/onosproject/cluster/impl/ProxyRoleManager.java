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
import org.onosproject.cluster.ProxyRoleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxy egress manager.
 */
@Component(immediate = true)
@Service
public class ProxyRoleManager implements ProxyRoleService {

    private static final String NODE_TYPE = System.getProperty("onos.node.type", "onos");

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterService clusterService;

    private NodeId localNodeId;
    private boolean proxyEnabled;
    private boolean isControllerNode;
    private boolean isProxyNode;

    @Activate
    public void activate() {
        localNodeId = clusterService.getLocalNode().id();
        proxyEnabled = NODE_TYPE.equalsIgnoreCase("proxy")
            || NODE_TYPE.equals("controller");
        isControllerNode = clusterService.getNodes().stream()
            .anyMatch(node -> node.id().equals(localNodeId));
        isProxyNode = clusterService.getProxyNodes().stream()
            .anyMatch(node -> node.id().equals(localNodeId));
        log.info("isControllerNode: {}", isControllerNode);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        log.info("Stopped");
    }

    @Override
    public boolean isProxyEnabled() {
        return proxyEnabled;
    }

    @Override
    public boolean isControllerNode() {
        return isControllerNode;
    }

    @Override
    public boolean isProxyNode() {
        return isProxyNode;
    }

    @Override
    public Set<NodeId> getControllerNodes() {
        if (!proxyEnabled || !isProxyNode) {
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
            .filter(id -> proxyNodeIds.get(Math.abs(id.id().hashCode() % proxyNodeIds.size())).equals(localNodeId))
            .collect(Collectors.toSet());
    }

    @Override
    public NodeId getProxyNode() {
        if (!proxyEnabled || !isControllerNode) {
            return null;
        }

        // TODO: This computation should be done each time a node joins/leaves the cluster.
        Set<Node> proxyNodes = clusterService.getProxyNodes();

        List<NodeId> proxyNodeIds = proxyNodes.stream()
            .map(Node::id)
            .sorted(Comparator.comparing(NodeId::id))
            .collect(Collectors.toList());

        return proxyNodeIds.get(Math.abs(localNodeId.id().hashCode() % proxyNodeIds.size()));
    }
}
