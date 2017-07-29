/*
 * Copyright 2017-present Open Networking Laboratory
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
import org.onosproject.core.Version;
import org.onosproject.event.AbstractEvent;

/**
 * Describes upgrade event.
 */
public class UpgradeEvent extends AbstractEvent<UpgradeEvent.Type, Version> {

    /**
     * Type of cluster-related events.
     */
    public enum Type {
        /**
         * Indicates an upgrade has been started.
         */
        UPGRADE_STARTED,

        /**
         * Indicates an upgrade is complete.
         */
        UPGRADE_COMPLETE
    }

    /**
     * Creates an event of a given type and for the specified version and the
     * current time.
     *
     * @param type    upgrade event type
     * @param version upgrade version
     */
    public UpgradeEvent(Type type, Version version) {
        super(type, version);
    }

    /**
     * Creates an event of a given type and for the specified version and time.
     *
     * @param type    upgrade event type
     * @param version upgrade version
     * @param time    occurrence time
     */
    public UpgradeEvent(Type type, Version version, long time) {
        super(type, version, time);
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
        if (obj instanceof UpgradeEvent) {
            final UpgradeEvent other = (UpgradeEvent) obj;
            return Objects.equals(this.type(), other.type()) &&
                    Objects.equals(this.subject(), other.subject()) &&
                    Objects.equals(this.time(), other.time());
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
