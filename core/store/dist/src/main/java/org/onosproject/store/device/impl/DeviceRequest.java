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
package org.onosproject.store.device.impl;

import java.util.Collection;

import org.onosproject.cluster.NodeId;
import org.onosproject.net.DeviceId;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Message to request for other peers information.
 */
public class DeviceRequest {

    private final NodeId sender;
    private final Collection<DeviceId> devices;

    public DeviceRequest(NodeId sender, Collection<DeviceId> devices) {
        this.sender = checkNotNull(sender);
        this.devices = checkNotNull(devices);
    }

    public NodeId sender() {
        return sender;
    }

    public Collection<DeviceId> devices() {
        return devices;
    }

    // For serializer
    @SuppressWarnings("unused")
    private DeviceRequest() {
        this.sender = null;
        this.devices = null;
    }
}
