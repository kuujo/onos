/*
 * Copyright 2016-present Open Networking Foundation
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

import java.util.Dictionary;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.GroupLeadershipEvent;
import org.onosproject.cluster.GroupLeadershipStore;
import org.onosproject.cluster.GroupLeadershipStoreDelegate;
import org.onosproject.cluster.Leadership;
import org.onosproject.cluster.Member;
import org.onosproject.cluster.MembershipGroupId;
import org.onosproject.cluster.MembershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.Version;
import org.onosproject.event.Change;
import org.onosproject.store.AbstractStore;
import org.onosproject.store.service.CoordinationService;
import org.onosproject.store.service.DistributedPrimitive.Status;
import org.onosproject.store.service.LeaderElector;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.felix.scr.annotations.ReferenceCardinality.MANDATORY_UNARY;
import static org.onlab.util.Tools.get;
import static org.onlab.util.Tools.groupedThreads;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of {@code LeadershipStore} that makes use of a {@link LeaderElector}
 * primitive.
 */
@Service
@Component(immediate = true)
public class DistributedGroupLeadershipStore
    extends AbstractStore<GroupLeadershipEvent, GroupLeadershipStoreDelegate>
    implements GroupLeadershipStore {

    private static final Pattern TOPIC_PATTERN = Pattern.compile("([^|]+)\\|[^|]+");
    private static final Pattern VERSION_PATTERN = Pattern.compile("[^|]+\\|([^|]+)");
    private static final char VERSION_SEP = '|';

    private final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoordinationService storageService;

    @Reference(cardinality = MANDATORY_UNARY)
    protected ComponentConfigService configService;

    @Reference(cardinality = MANDATORY_UNARY)
    protected MembershipService membershipService;

    private static final long DEFAULT_ELECTION_TIMEOUT_MILLIS = 250;
    @Property(name = "electionTimeoutMillis", longValue = DEFAULT_ELECTION_TIMEOUT_MILLIS,
        label = "the leader election timeout in milliseconds")
    private long electionTimeoutMillis = DEFAULT_ELECTION_TIMEOUT_MILLIS;

    private ExecutorService statusChangeHandler;
    private NodeId localNodeId;
    private LeaderElector leaderElector;
    private final Map<String, Leadership> localLeaderCache = Maps.newConcurrentMap();

    private final Consumer<Change<Leadership>> leadershipChangeListener = change -> {
        Leadership oldValue = change.oldValue();
        Leadership newValue = change.newValue();

        boolean leaderChanged = !Objects.equals(oldValue.leader(), newValue.leader());
        boolean candidatesChanged = !Objects.equals(oldValue.candidates(), newValue.candidates());

        GroupLeadershipEvent.Type eventType = null;
        if (leaderChanged && candidatesChanged) {
            eventType = GroupLeadershipEvent.Type.LEADER_AND_CANDIDATES_CHANGED;
        }
        if (leaderChanged && !candidatesChanged) {
            eventType = GroupLeadershipEvent.Type.LEADER_CHANGED;
        }
        if (!leaderChanged && candidatesChanged) {
            eventType = GroupLeadershipEvent.Type.CANDIDATES_CHANGED;
        }
        notifyDelegate(new GroupLeadershipEvent(eventType, new Leadership(
            parseTopic(change.newValue().topic()),
            change.newValue().leader(),
            change.newValue().candidates()),
            parseGroup(change.newValue().topic())));
        // Update local cache of currently held leaderships
        if (Objects.equals(newValue.leaderNodeId(), localNodeId)) {
            localLeaderCache.put(newValue.topic(), newValue);
        } else {
            localLeaderCache.remove(newValue.topic());
        }
    };

    private final Consumer<Status> clientStatusListener = status ->
        statusChangeHandler.execute(() -> handleStatusChange(status));

    private void handleStatusChange(Status status) {
        // Notify mastership Service of disconnect and reconnect
        if (status == Status.ACTIVE) {
            // Service Restored
            localLeaderCache.forEach((topic, leadership) -> leaderElector.run(topic, localNodeId));
            leaderElector.getLeaderships().forEach((topic, leadership) ->
                notifyDelegate(new GroupLeadershipEvent(
                    GroupLeadershipEvent.Type.SERVICE_RESTORED,
                    new Leadership(
                        parseTopic(leadership.topic()),
                        leadership.leader(),
                        leadership.candidates()),
                    parseGroup(leadership.topic()))));
        } else if (status == Status.SUSPENDED) {
            // Service Suspended
            localLeaderCache.forEach((topic, leadership) ->
                notifyDelegate(new GroupLeadershipEvent(
                    GroupLeadershipEvent.Type.SERVICE_DISRUPTED,
                    new Leadership(
                        parseTopic(leadership.topic()),
                        leadership.leader(),
                        leadership.candidates()),
                    parseGroup(leadership.topic()))));
        } else {
            // Should be only inactive state
            return;
        }
    }

    @Activate
    public void activate() {
        configService.registerProperties(getClass());
        statusChangeHandler = Executors.newSingleThreadExecutor(
            groupedThreads("onos/store/dist/cluster/leadership", "status-change-handler", log));
        localNodeId = clusterService.getLocalNode().id();
        leaderElector = storageService.leaderElectorBuilder()
            .withName("onos-leadership-elections")
            .withElectionTimeout(electionTimeoutMillis)
            .build()
            .asLeaderElector();
        leaderElector.addChangeListener(leadershipChangeListener);
        leaderElector.addStatusChangeListener(clientStatusListener);
        log.info("Started");
    }

    @Modified
    public void modified(ComponentContext context) {
        if (context == null) {
            return;
        }

        Dictionary<?, ?> properties = context.getProperties();
        long newElectionTimeoutMillis;
        try {
            String s = get(properties, "electionTimeoutMillis");
            newElectionTimeoutMillis = isNullOrEmpty(s) ? electionTimeoutMillis : Long.parseLong(s.trim());
        } catch (NumberFormatException | ClassCastException e) {
            log.warn("Malformed configuration detected; using defaults", e);
            newElectionTimeoutMillis = DEFAULT_ELECTION_TIMEOUT_MILLIS;
        }

        if (newElectionTimeoutMillis != electionTimeoutMillis) {
            electionTimeoutMillis = newElectionTimeoutMillis;
            leaderElector = storageService.leaderElectorBuilder()
                .withName("onos-leadership-elections")
                .withElectionTimeout(electionTimeoutMillis)
                .build()
                .asLeaderElector();
        }
    }

    @Deactivate
    public void deactivate() {
        leaderElector.removeChangeListener(leadershipChangeListener);
        leaderElector.removeStatusChangeListener(clientStatusListener);
        statusChangeHandler.shutdown();
        configService.unregisterProperties(getClass(), false);
        log.info("Stopped");
    }

    @Override
    public Leadership addRegistration(String topic) {
        return leaderElector.run(getGroupTopic(topic, membershipService.getLocalGroupId()), localNodeId);
    }

    @Override
    public void removeRegistration(String topic) {
        leaderElector.withdraw(getGroupTopic(topic, membershipService.getLocalGroupId()));
    }

    @Override
    public void removeRegistration(NodeId nodeId) {
        leaderElector.evict(nodeId);
    }

    @Override
    public boolean moveLeadership(String topic, NodeId toNodeId) {
        Member member = membershipService.getMember(toNodeId);
        return member != null && leaderElector.anoint(getGroupTopic(topic, member.groupId()), toNodeId);
    }

    @Override
    public boolean makeTopCandidate(String topic, NodeId nodeId) {
        Member member = membershipService.getMember(nodeId);
        return member != null && leaderElector.promote(getGroupTopic(topic, member.groupId()), nodeId);
    }

    @Override
    public Leadership getLeadership(String topic, MembershipGroupId groupId) {
        Leadership leadership = leaderElector.getLeadership(getGroupTopic(topic, groupId));
        return leadership != null ? new Leadership(
            parseTopic(leadership.topic()),
            leadership.leader(),
            leadership.candidates()) : null;
    }

    @Override
    public Map<String, Leadership> getLeaderships(MembershipGroupId groupId) {
        Map<String, Leadership> leaderships = leaderElector.getLeaderships();
        return leaderships.entrySet().stream()
            .filter(e -> isGroupTopic(e.getKey(), groupId))
            .collect(Collectors.toMap(e -> parseTopic(e.getKey()),
                e -> new Leadership(parseTopic(e.getKey()), e.getValue().leader(), e.getValue().candidates())));
    }

    /**
     * Returns the versioned topic for the given node.
     *
     * @param topic the topic for the given node
     * @return the versioned topic for the given node
     */
    private String getGroupTopic(String topic, MembershipGroupId groupId) {
        return topic + VERSION_SEP + groupId.id().toString();
    }

    /**
     * Returns whether the given topic is a topic for the current cluster version.
     *
     * @param topic the topic to check
     * @return whether the given topic is relevant to the current cluster version
     */
    private boolean isGroupTopic(String topic, MembershipGroupId groupId) {
        return topic.endsWith(VERSION_SEP + groupId.id().toString());
    }

    /**
     * Parses a topic string, returning the base topic.
     *
     * @param topic the topic string to parse
     * @return the base topic string
     */
    private String parseTopic(String topic) {
        Matcher m = TOPIC_PATTERN.matcher(topic);
        if (m.matches()) {
            return m.group(1);
        } else {
            throw new IllegalArgumentException("Invalid versioned leadership topic: " + topic);
        }
    }

    /**
     * Parses a topic string, returning the base version.
     *
     * @param topic the topic string to parse
     * @return the base version
     */
    private MembershipGroupId parseGroup(String topic) {
        Matcher m = VERSION_PATTERN.matcher(topic);
        if (m.matches()) {
            return membershipService.getGroupId(Version.version(m.group(1)));
        } else {
            throw new IllegalArgumentException("Invalid versioned leadership topic: " + topic);
        }
    }
}
