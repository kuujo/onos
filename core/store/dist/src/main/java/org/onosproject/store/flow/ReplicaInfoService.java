/*
 * Copyright 2014-present Open Networking Foundation
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
package org.onosproject.store.flow;

import org.onosproject.cluster.NodeId;
import org.onosproject.net.DeviceId;

/**
 * Service to return where the replica should be placed.
 */
public interface ReplicaInfoService {

    /**
     * Returns the placement information for given Device.
     *
     * @param deviceId identifier of the device
     * @return placement information
     */
    ReplicaInfo getReplicaInfoFor(DeviceId deviceId);

    /**
     * Returns the master for the given device.
     *
     * @param deviceId the device identifier
     * @return the master for the given device
     */
    default NodeId getMasterFor(DeviceId deviceId) {
        ReplicaInfo replicaInfo = getReplicaInfoFor(deviceId);
        return replicaInfo != null ? replicaInfo.master().orElse(null) : null;
    }

    /**
     * Returns a boolean indicating whether the local node is the master for the given device.
     *
     * @param deviceId the identifier of the device
     * @return indicates whether the local node is the master for the given device
     */
    boolean isLocalMaster(DeviceId deviceId);

    /**
     * Adds the specified replica placement info change listener.
     *
     * @param listener the replica placement info change listener
     */
    void addListener(ReplicaInfoEventListener listener);

    /**
     * Removes the specified replica placement info change listener.
     *
     * @param listener the replica placement info change listener
     */
    void removeListener(ReplicaInfoEventListener listener);

}
