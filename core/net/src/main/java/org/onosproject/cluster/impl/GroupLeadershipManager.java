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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.GroupLeadershipAdminService;
import org.onosproject.cluster.GroupLeadershipService;
import org.onosproject.cluster.GroupLeadershipStore;
import org.onosproject.cluster.Leadership;
import org.onosproject.cluster.LeadershipAdminService;
import org.onosproject.cluster.LeadershipEvent;
import org.onosproject.cluster.LeadershipEventListener;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.LeadershipStoreDelegate;
import org.onosproject.cluster.MembershipGroupId;
import org.onosproject.cluster.MembershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.event.AbstractListenerManager;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of {@link LeadershipService} and {@link LeadershipAdminService}.
 */
@Component(immediate = true)
@Service
public class GroupLeadershipManager
    extends AbstractListenerManager<LeadershipEvent, LeadershipEventListener>
    implements GroupLeadershipService, GroupLeadershipAdminService {

    private final Logger log = getLogger(getClass());

    private LeadershipStoreDelegate delegate = this::post;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected GroupLeadershipStore store;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected MembershipService membershipService;

    private NodeId localNodeId;

    @Activate
    public void activate() {
        localNodeId = clusterService.getLocalNode().id();
        store.setDelegate(delegate);
        eventDispatcher.addSink(LeadershipEvent.class, listenerRegistry);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        Maps.filterValues(store.getLeaderships(membershipService.getLocalGroupId()),
            v -> v.candidates().contains(localNodeId))
            .keySet()
            .forEach(this::withdraw);
        store.unsetDelegate(delegate);
        eventDispatcher.removeSink(LeadershipEvent.class);
        log.info("Stopped");
    }

    @Override
    public NodeId getLeader(String topic) {
        return getLeader(topic, membershipService.getLocalGroupId());
    }

    @Override
    public Leadership getLeadership(String topic) {
        return getLeadership(topic, membershipService.getLocalGroupId());
    }

    @Override
    public Leadership getLeadership(String topic, MembershipGroupId groupId) {
        return store.getLeadership(topic, groupId);
    }

    @Override
    public List<NodeId> getCandidates(String topic) {
        return getCandidates(topic, membershipService.getLocalGroupId());
    }

    @Override
    public Leadership runForLeadership(String topic) {
        return store.addRegistration(topic);
    }

    @Override
    public void withdraw(String topic) {
        store.removeRegistration(topic);
    }

    @Override
    public Map<String, Leadership> getLeaderBoard(MembershipGroupId groupId) {
        return store.getLeaderships(groupId);
    }

    @Override
    public boolean transferLeadership(String topic, NodeId to) {
        return store.moveLeadership(topic, to);
    }

    @Override
    public void unregister(NodeId nodeId) {
        store.removeRegistration(nodeId);
    }

    @Override
    public boolean promoteToTopOfCandidateList(String topic, NodeId nodeId) {
        return store.makeTopCandidate(topic, nodeId);
    }
}
