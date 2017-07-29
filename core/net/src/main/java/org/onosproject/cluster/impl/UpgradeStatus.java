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
package org.onosproject.cluster.impl;

import org.onosproject.core.Version;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Cluster upgrade status.
 */
public class UpgradeStatus {

    /**
     * Upgrade state.
     */
    public enum State {
        /**
         * Indicates that the upgrade process is in progress.
         */
        IN_PROGRESS,

        /**
         * Indicates that the upgrade process is complete.
         */
        COMPLETE
    }

    private final Version version;
    private final State state;

    public UpgradeStatus() {
        this.version = null;
        this.state = null;
    }

    public UpgradeStatus(Version version, State state) {
        this.version = version;
        this.state = state;
    }

    /**
     * Returns the upgrade service version.
     *
     * @return the upgrade service version
     */
    public Version version() {
        return version;
    }

    /**
     * Returns the upgrade state.
     *
     * @return the upgrade state
     */
    public State state() {
        return state;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("version", version)
                .add("state", state)
                .toString();
    }
}
