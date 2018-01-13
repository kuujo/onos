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
package org.onosproject.cluster;

import java.util.List;
import java.util.Map;

/**
 * Group leadership service adapter.
 */
public class GroupLeadershipServiceAdapter implements GroupLeadershipService {
    @Override
    public NodeId getLeader(String topic) {
        return null;
    }

    @Override
    public Leadership getLeadership(String topic) {
        return null;
    }

    @Override
    public Leadership getLeadership(String topic, MembershipGroupId groupId) {
        return null;
    }

    @Override
    public List<NodeId> getCandidates(String topic) {
        return null;
    }

    @Override
    public Leadership runForLeadership(String topic) {
        return null;
    }

    @Override
    public void withdraw(String topic) {

    }

    @Override
    public Map<String, Leadership> getLeaderBoard(MembershipGroupId groupId) {
        return null;
    }

    @Override
    public void addListener(LeadershipEventListener listener) {

    }

    @Override
    public void removeListener(LeadershipEventListener listener) {

    }
}
