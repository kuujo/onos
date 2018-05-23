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
package org.onosproject.store.primitives.impl;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.utils.net.Address;
import org.onosproject.cluster.ClusterEventListener;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.NodeId;

import static io.atomix.cluster.ClusterMembershipEvent.Type.MEMBER_ADDED;
import static io.atomix.cluster.ClusterMembershipEvent.Type.MEMBER_REMOVED;

/**
 * Atomix cluster membership service.
 */
public class AtomixClusterMembershipService implements ClusterMembershipService {
    private final ClusterService clusterService;
    private final Map<ClusterMembershipEventListener, ClusterEventListener> listenerMap = Maps.newIdentityHashMap();

    public AtomixClusterMembershipService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    private Member toMember(ControllerNode node) {
        return Member.member(MemberId.from(node.id().id()), Address.from(node.ip().toString(), node.tcpPort()));
    }

    @Override
    public Member getLocalMember() {
        return toMember(clusterService.getLocalNode());
    }

    @Override
    public Set<Member> getMembers() {
        return clusterService.getNodes()
            .stream()
            .map(this::toMember)
            .collect(Collectors.toSet());
    }

    @Override
    public Member getMember(MemberId memberId) {
        ControllerNode node = clusterService.getNode(NodeId.nodeId(memberId.id()));
        return node != null ? toMember(node) : null;
    }

    @Override
    public synchronized void addListener(ClusterMembershipEventListener listener) {
        ClusterEventListener clusterEventListener = event -> {
            switch (event.type()) {
                case INSTANCE_ACTIVATED:
                    listener.onEvent(new ClusterMembershipEvent(MEMBER_ADDED, toMember(event.subject())));
                    break;
                case INSTANCE_DEACTIVATED:
                    listener.onEvent(new ClusterMembershipEvent(MEMBER_REMOVED, toMember(event.subject())));
                    break;
                default:
                    break;
            }
        };
        listenerMap.put(listener, clusterEventListener);
        clusterService.addListener(clusterEventListener);
    }

    @Override
    public synchronized void removeListener(ClusterMembershipEventListener listener) {
        ClusterEventListener clusterEventListener = listenerMap.remove(listener);
        if (clusterEventListener != null) {
            clusterService.removeListener(clusterEventListener);
        }
    }
}
