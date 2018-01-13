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
package org.onosproject.store.flow.impl;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.GroupLeadershipService;
import org.onosproject.cluster.Leadership;
import org.onosproject.cluster.LeadershipEvent;
import org.onosproject.cluster.LeadershipEventListener;
import org.onosproject.event.AbstractListenerManager;
import org.onosproject.net.DeviceId;
import org.onosproject.store.flow.ReplicaInfo;
import org.onosproject.store.flow.ReplicaInfoEvent;
import org.onosproject.store.flow.ReplicaInfoEventListener;
import org.onosproject.store.flow.ReplicaInfoService;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.onosproject.store.flow.ReplicaInfoEvent.Type.BACKUPS_CHANGED;
import static org.onosproject.store.flow.ReplicaInfoEvent.Type.MASTER_CHANGED;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Manages replica placement information.
 */
@Component(immediate = true)
@Service
public class ReplicaInfoManager
    extends AbstractListenerManager<ReplicaInfoEvent, ReplicaInfoEventListener>
    implements ReplicaInfoService {

    private static final Pattern DEVICE_MASTERSHIP_TOPIC_PATTERN = Pattern.compile("device:([^|]+)");

    private final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected GroupLeadershipService leadershipService;

    private final LeadershipEventListener leadershipChangeListener = event -> {
        String topic = event.subject().topic();
        if (!isDeviceMastershipTopic(topic)) {
            return;
        }

        DeviceId deviceId = extractDeviceIdFromTopic(topic);
        ReplicaInfo replicaInfo = buildFromLeadership(event.subject());

        if (event.type() == LeadershipEvent.Type.LEADER_CHANGED) {
            post(new ReplicaInfoEvent(MASTER_CHANGED, deviceId, replicaInfo));
        } else if (event.type() == LeadershipEvent.Type.CANDIDATES_CHANGED) {
            post(new ReplicaInfoEvent(BACKUPS_CHANGED, deviceId, replicaInfo));
        } else if (event.type() == LeadershipEvent.Type.LEADER_AND_CANDIDATES_CHANGED) {
            post(new ReplicaInfoEvent(MASTER_CHANGED, deviceId, replicaInfo));
            post(new ReplicaInfoEvent(BACKUPS_CHANGED, deviceId, replicaInfo));
        }
    };

    @Activate
    public void activate() {
        eventDispatcher.addSink(ReplicaInfoEvent.class, listenerRegistry);
        leadershipService.addListener(leadershipChangeListener);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        eventDispatcher.removeSink(ReplicaInfoEvent.class);
        leadershipService.removeListener(leadershipChangeListener);
        log.info("Stopped");
    }

    @Override
    public ReplicaInfo getReplicaInfoFor(DeviceId deviceId) {
        return buildFromLeadership(leadershipService.getLeadership(createDeviceMastershipTopic(deviceId)));
    }

    @Override
    public void addListener(ReplicaInfoEventListener listener) {
        listenerRegistry.addListener(checkNotNull(listener));
    }

    @Override
    public void removeListener(ReplicaInfoEventListener listener) {
        listenerRegistry.removeListener(checkNotNull(listener));
    }

    String createDeviceMastershipTopic(DeviceId deviceId) {
        return String.format("device:%s", deviceId.toString());
    }

    DeviceId extractDeviceIdFromTopic(String topic) {
        Matcher m = DEVICE_MASTERSHIP_TOPIC_PATTERN.matcher(topic);
        if (m.matches()) {
            return DeviceId.deviceId(m.group(1));
        } else {
            throw new IllegalArgumentException("Invalid device mastership topic: " + topic);
        }
    }

    boolean isDeviceMastershipTopic(String topic) {
        Matcher m = DEVICE_MASTERSHIP_TOPIC_PATTERN.matcher(topic);
        return m.matches();
    }

    static ReplicaInfo buildFromLeadership(Leadership leadership) {
        return new ReplicaInfo(leadership.leaderNodeId(), leadership.candidates().stream()
            .filter(nodeId -> !Objects.equals(nodeId, leadership.leaderNodeId()))
            .collect(Collectors.toList()));
    }
}
