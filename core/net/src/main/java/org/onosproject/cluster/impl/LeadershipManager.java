/*
 * Copyright 2016-present Open Networking Foundation
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

import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.GroupLeadershipAdminService;
import org.onosproject.cluster.GroupLeadershipEventListener;
import org.onosproject.cluster.GroupLeadershipService;
import org.onosproject.cluster.Leadership;
import org.onosproject.cluster.LeadershipAdminService;
import org.onosproject.cluster.LeadershipEvent;
import org.onosproject.cluster.LeadershipEventListener;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.MembershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.event.AbstractListenerManager;
import org.onosproject.upgrade.UpgradeEvent;
import org.onosproject.upgrade.UpgradeEventListener;
import org.onosproject.upgrade.UpgradeService;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of {@link LeadershipService} and {@link LeadershipAdminService}.
 */
@Component(immediate = true)
@Service
public class LeadershipManager
    extends AbstractListenerManager<LeadershipEvent, LeadershipEventListener>
    implements LeadershipService, LeadershipAdminService {

    private final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected GroupLeadershipService groupLeadershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected GroupLeadershipAdminService groupLeadershipAdminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected MembershipService membershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected UpgradeService upgradeService;

    private final GroupLeadershipEventListener groupLeadershipEventListener = event -> {
        // Only post the event if it matches the local node's version.
        if (event.groupId().equals(membershipService.getLocalGroupId())) {
            post(new LeadershipEvent(LeadershipEvent.Type.valueOf(event.type().name()), event.subject(), event.time()));
        }
    };

    private final UpgradeEventListener upgradeEventListener = event -> {
        // If the cluster version was changed by an upgrade event, trigger leadership events for the new version.
        if (event.type() == UpgradeEvent.Type.UPGRADED || event.type() == UpgradeEvent.Type.ROLLED_BACK) {
            // Iterate through all current leaderships for the new version and trigger events.
            for (Leadership leadership : getLeaderBoard().values()) {
                post(new LeadershipEvent(
                    LeadershipEvent.Type.LEADER_AND_CANDIDATES_CHANGED,
                    leadership,
                    event.time()));
            }
        }
    };

    @Activate
    public void activate() {
        eventDispatcher.addSink(LeadershipEvent.class, listenerRegistry);
        groupLeadershipService.addListener(groupLeadershipEventListener);
        upgradeService.addListener(upgradeEventListener);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        eventDispatcher.removeSink(LeadershipEvent.class);
        groupLeadershipService.removeListener(groupLeadershipEventListener);
        upgradeService.removeListener(upgradeEventListener);
        log.info("Stopped");
    }

    @Override
    public Leadership getLeadership(String topic) {
        return groupLeadershipService.getLeadership(topic, upgradeService.getActiveGroup());
    }

    @Override
    public Leadership runForLeadership(String topic) {
        groupLeadershipService.runForLeadership(topic);
        return getLeadership(topic);
    }

    @Override
    public void withdraw(String topic) {
        groupLeadershipService.withdraw(topic);
    }

    @Override
    public Map<String, Leadership> getLeaderBoard() {
        return groupLeadershipService.getLeaderBoard(upgradeService.getActiveGroup());
    }

    @Override
    public boolean transferLeadership(String topic, NodeId to) {
        return groupLeadershipAdminService.transferLeadership(topic, to);
    }

    @Override
    public boolean promoteToTopOfCandidateList(String topic, NodeId nodeId) {
        return groupLeadershipAdminService.promoteToTopOfCandidateList(topic, nodeId);
    }

    @Override
    public void unregister(NodeId nodeId) {
        groupLeadershipAdminService.unregister(nodeId);
    }
}
