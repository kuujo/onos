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

import org.onosproject.core.Version;
import org.onosproject.store.Store;

/**
 * Upgrade store.
 */
public interface UpgradeStore extends Store<UpgradeEvent, UpgradeStoreDelegate> {

    /**
     * Returns the current cluster version.
     *
     * @return the current cluster version
     */
    Version getCurrentVersion();

    /**
     * Sets the current version.
     *
     * @param currentVersion the current version
     * @return indicates whether the current version was set for the first time
     */
    boolean setCurrentVersion(Version currentVersion);

    /**
     * Begins an upgrade to the given version.
     *
     * @param upgradeVersion the version to which to begin the upgrade
     * @return indicates whether the upgrade was started
     */
    boolean beginUpgrade(Version upgradeVersion);

    /**
     * Completes an upgrade.
     *
     * @return indicates whether the upgrade was completed
     */
    boolean completeUpgrade();

    /**
     * Rolls back the current upgrade.
     *
     * @return indicates whether the rollback was successful
     */
    boolean rollbackUpgrade();

}
