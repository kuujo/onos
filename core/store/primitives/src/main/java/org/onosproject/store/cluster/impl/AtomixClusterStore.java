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
package org.onosproject.store.cluster.impl;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.Member;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onlab.packet.IpAddress;
import org.onosproject.cluster.ClusterEvent;
import org.onosproject.cluster.ClusterMetadataEvent;
import org.onosproject.cluster.ClusterMetadataEventListener;
import org.onosproject.cluster.ClusterMetadataService;
import org.onosproject.cluster.ClusterStore;
import org.onosproject.cluster.ClusterStoreDelegate;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.DefaultControllerNode;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.Version;
import org.onosproject.store.AbstractStore;
import org.onosproject.store.impl.AtomixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix cluster store.
 */
@Component(immediate = true)
@Service
public class AtomixClusterStore extends AbstractStore<ClusterEvent, ClusterStoreDelegate> implements ClusterStore {
    private static final String INSTANCE_ID_NULL = "Instance ID cannot be null";

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected AtomixManager atomixManager;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterMetadataService metadataService;

    private AtomixCluster atomix;
    private ControllerNode localNode;
    private final Map<NodeId, ControllerNode> nodes = Maps.newConcurrentMap();
    private final Map<NodeId, ControllerNode.State> states = Maps.newConcurrentMap();
    private final Map<NodeId, Version> versions = Maps.newConcurrentMap();
    private final Map<NodeId, Instant> updates = Maps.newConcurrentMap();
    private final ClusterMetadataEventListener metadataEventListener = this::changeMetadata;
    private final ClusterMembershipEventListener membershipEventListener = this::changeMembership;

    @Activate
    public void activate() {
        metadataService.addListener(metadataEventListener);
        this.atomix = atomixManager.getAtomix();
        atomix.membershipService().addListener(membershipEventListener);
        metadataService.getClusterMetadata().getNodes().forEach(node -> {
            nodes.put(node.id(), node);
            updates.put(node.id(), Instant.now());
        });
        localNode = metadataService.getLocalNode();
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        metadataService.removeListener(metadataEventListener);
        atomix.membershipService().removeListener(membershipEventListener);
        log.info("Stopped");
    }

    private void changeMetadata(ClusterMetadataEvent event) {
        event.subject().getNodes().forEach(node -> nodes.putIfAbsent(node.id(), node));
        nodes.entrySet().removeIf(entry -> !event.subject().getNodes().contains(entry.getValue()));
    }

    private void changeMembership(ClusterMembershipEvent event) {
        ControllerNode node = nodes.get(NodeId.nodeId(event.subject().id().id()));
        if (node != null) {
            switch (event.type()) {
                case MEMBER_ADDED:
                case MEMBER_UPDATED:
                    updateState(node, event.subject());
                    updateVersion(node, event.subject());
                    break;
                case MEMBER_REMOVED:
                    if (states.put(node.id(), ControllerNode.State.INACTIVE) != ControllerNode.State.INACTIVE) {
                        notifyDelegate(new ClusterEvent(ClusterEvent.Type.INSTANCE_DEACTIVATED, node));
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private void updateState(ControllerNode node, Member member) {
        String state = member.metadata().get("state");
        if (state != null) {
            if (state.equals(ControllerNode.State.ACTIVE.name())) {
                if (states.put(node.id(), ControllerNode.State.ACTIVE) != ControllerNode.State.ACTIVE) {
                    markUpdated(node.id());
                    notifyDelegate(new ClusterEvent(ClusterEvent.Type.INSTANCE_ACTIVATED, node));
                }
            } else if (state.equals(ControllerNode.State.READY.name())) {
                if (states.put(node.id(), ControllerNode.State.READY) != ControllerNode.State.READY) {
                    notifyDelegate(new ClusterEvent(ClusterEvent.Type.INSTANCE_READY, node));
                }
            } else {
                if (states.put(node.id(), ControllerNode.State.ACTIVE) != ControllerNode.State.ACTIVE) {
                    notifyDelegate(new ClusterEvent(ClusterEvent.Type.INSTANCE_ACTIVATED, node));
                }
            }
        } else {
            if (states.put(node.id(), ControllerNode.State.ACTIVE) != ControllerNode.State.ACTIVE) {
                notifyDelegate(new ClusterEvent(ClusterEvent.Type.INSTANCE_ACTIVATED, node));
            }
        }
    }

    private void updateVersion(ControllerNode node, Member member) {
        String version = member.metadata().get("version");
        if (version != null) {
            versions.put(node.id(), Version.version(version));
        }
    }

    private void markUpdated(NodeId nodeId) {
        updates.put(nodeId, Instant.now());
    }

    private ControllerNode toNode(Member member) {
        return new DefaultControllerNode(
            NodeId.nodeId(member.id().id()),
            IpAddress.valueOf(member.address().address()),
            member.address().port());
    }

    @Override
    public ControllerNode getLocalNode() {
        return toNode(atomix.membershipService().getLocalMember());
    }

    @Override
    public Set<ControllerNode> getNodes() {
        return atomix.membershipService().getMembers().stream().map(this::toNode).collect(Collectors.toSet());
    }

    @Override
    public ControllerNode getNode(NodeId nodeId) {
        Member member = atomix.membershipService().getMember(nodeId.id());
        return member != null ? toNode(member) : null;
    }

    @Override
    public ControllerNode.State getState(NodeId nodeId) {
        checkNotNull(nodeId, INSTANCE_ID_NULL);
        return states.get(nodeId);
    }

    @Override
    public Version getVersion(NodeId nodeId) {
        checkNotNull(nodeId, INSTANCE_ID_NULL);
        return versions.get(nodeId);
    }

    @Override
    public Instant getLastUpdatedInstant(NodeId nodeId) {
        checkNotNull(nodeId, INSTANCE_ID_NULL);
        return updates.get(nodeId);
    }

    @Override
    public void markFullyStarted(boolean started) {
        ControllerNode.State state = started ? ControllerNode.State.READY : ControllerNode.State.ACTIVE;
        states.put(localNode.id(), state);
        atomix.membershipService().getLocalMember().metadata().put("state", state.name());
    }

    @Override
    public ControllerNode addNode(NodeId nodeId, IpAddress ip, int tcpPort) {
        checkNotNull(nodeId, INSTANCE_ID_NULL);
        ControllerNode node = new DefaultControllerNode(nodeId, ip, tcpPort);
        nodes.put(node.id(), node);
        ControllerNode.State state = node.equals(localNode)
            ? ControllerNode.State.ACTIVE : ControllerNode.State.INACTIVE;
        atomix.membershipService().getMember(node.id().id()).metadata().put("state", state.name());
        notifyDelegate(new ClusterEvent(ClusterEvent.Type.INSTANCE_ADDED, node));
        return node;
    }

    @Override
    public void removeNode(NodeId nodeId) {
        checkNotNull(nodeId, INSTANCE_ID_NULL);
        ControllerNode node = nodes.remove(nodeId);
        if (node != null) {
            states.remove(nodeId);
            notifyDelegate(new ClusterEvent(ClusterEvent.Type.INSTANCE_REMOVED, node));
        }
    }
}
