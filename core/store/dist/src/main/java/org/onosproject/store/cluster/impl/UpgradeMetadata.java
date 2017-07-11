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
package org.onosproject.store.cluster.impl;

import org.onosproject.core.Version;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Cluster upgrade status.
 */
public class UpgradeMetadata {

    /**
     * Upgrade state.
     */
    public enum State {
        /**
         * Indicates that the version is stable.
         */
        STABLE,

        /**
         * Indicates that the upgrade process is in progress.
         */
        IN_PROGRESS,

        /**
         * Indicates that the upgrade process is complete.
         */
        COMPLETE
    }

    private final Version currentVersion;
    private final Version upgradeVersion;
    private final State state;

    public UpgradeMetadata() {
        this.currentVersion = null;
        this.upgradeVersion = null;
        this.state = null;
    }

    public UpgradeMetadata(Version currentVersion, Version upgradeVersion, State state) {
        this.currentVersion = currentVersion;
        this.upgradeVersion = upgradeVersion;
        this.state = state;
    }

    /**
     * Returns the current cluster version.
     *
     * @return the current cluster version
     */
    public Version currentVersion() {
        return currentVersion;
    }

    /**
     * Returns the upgrade version.
     *
     * @return the upgrade version
     */
    public Version upgradeVersion() {
        return upgradeVersion;
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
                .add("currentVersion", currentVersion)
                .add("upgradeVersion", upgradeVersion)
                .add("state", state)
                .toString();
    }
}
