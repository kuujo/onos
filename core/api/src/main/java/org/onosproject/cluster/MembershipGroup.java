/*
 * Copyright 2017-present Open Networking Foundation
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

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Membership group.
 */
public class MembershipGroup {
    private final MembershipGroupId groupId;
    private final Set<Member> members;

    public MembershipGroup(MembershipGroupId groupId, Set<Member> members) {
        this.groupId = groupId;
        this.members = members;
    }

    /**
     * Returns the membership group identifier.
     *
     * @return the membership group identifier
     */
    public MembershipGroupId groupId() {
        return groupId;
    }

    /**
     * Returns the set of members in the group.
     *
     * @return the set of members in the group
     */
    public Set<Member> members() {
        return members;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("groupId", groupId)
            .add("members", members)
            .toString();
    }
}
