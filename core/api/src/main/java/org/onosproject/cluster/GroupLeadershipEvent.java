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

import java.util.Objects;

import com.google.common.base.MoreObjects;
import org.onosproject.event.AbstractEvent;

/**
 * Describes group leadership-related event.
 */
public class GroupLeadershipEvent extends AbstractEvent<GroupLeadershipEvent.Type, Leadership> {

    /**
     * Type of leadership events.
     */
    public enum Type {
        /**
         * Signifies a change in both the leader as well as change to the list of candidates. Keep in mind though that
         * the first node entering the race will trigger this event as it will become a candidate and automatically get
         * promoted to become leader.
         */
        LEADER_AND_CANDIDATES_CHANGED,

        /**
         * Signifies that the leader for a topic has changed.
         */
        LEADER_CHANGED,

        /**
         * Signifies a change in the list of candidates for a topic.
         */
        CANDIDATES_CHANGED,

        /**
         * Signifies the Leadership Elector is unavailable.
         */
        SERVICE_DISRUPTED,

        /**
         * Signifies the Leadership Elector is available again.
         */
        SERVICE_RESTORED
    }

    private final MembershipGroupId groupId;

    /**
     * Creates an event of a given type and for the specified instance and the
     * current time.
     *
     * @param type       leadership event type
     * @param leadership event subject
     * @param groupId    the event group identifier
     */
    public GroupLeadershipEvent(Type type, Leadership leadership, MembershipGroupId groupId) {
        super(type, leadership);
        this.groupId = groupId;
    }

    /**
     * Creates an event of a given type and for the specified subject and time.
     *
     * @param type       leadership event type
     * @param leadership event subject
     * @param groupId    the event group identifier
     * @param time       occurrence time
     */
    public GroupLeadershipEvent(Type type, Leadership leadership, MembershipGroupId groupId, long time) {
        super(type, leadership, time);
        this.groupId = groupId;
    }

    /**
     * Returns the event group identifier.
     *
     * @return the event group identifier
     */
    public MembershipGroupId groupId() {
        return groupId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type(), subject(), time());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof GroupLeadershipEvent) {
            final GroupLeadershipEvent other = (GroupLeadershipEvent) obj;
            return Objects.equals(this.type(), other.type())
                && Objects.equals(this.subject(), other.subject())
                && Objects.equals(this.groupId(), other.groupId())
                && Objects.equals(this.time(), other.time());
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass())
            .add("type", type())
            .add("subject", subject())
            .add("time", time())
            .toString();
    }
}
