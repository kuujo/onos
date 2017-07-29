/*
 * Copyright 2014-present Open Networking Laboratory
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
package org.onosproject.store.cluster.messaging.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onlab.util.KryoNamespace;
import org.onlab.util.Tools;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.NodeId;
import org.onosproject.store.LogicalTimestamp;
import org.onosproject.store.cluster.messaging.ClusterCommunicationService;
import org.onosproject.store.cluster.messaging.ClusterMessage;
import org.onosproject.store.cluster.messaging.ClusterMessageHandler;
import org.onosproject.store.cluster.messaging.Endpoint;
import org.onosproject.store.cluster.messaging.MessageSubject;
import org.onosproject.store.cluster.messaging.MessagingException;
import org.onosproject.store.cluster.messaging.MessagingService;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.WallClockTimestamp;
import org.onosproject.utils.MeteringAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.security.AppGuard.checkPermission;
import static org.onosproject.security.AppPermission.Type.CLUSTER_WRITE;

@Component(immediate = true)
@Service
public class ClusterCommunicationManager
        implements ClusterCommunicationService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final MeteringAgent subjectMeteringAgent = new MeteringAgent(PRIMITIVE_NAME, SUBJECT_PREFIX, true);
    private final MeteringAgent endpointMeteringAgent = new MeteringAgent(PRIMITIVE_NAME, ENDPOINT_PREFIX, true);

    private static final String PRIMITIVE_NAME = "clusterCommunication";
    private static final String SUBJECT_PREFIX = "subject";
    private static final String ENDPOINT_PREFIX = "endpoint";

    private static final String SERIALIZING = "serialization";
    private static final String DESERIALIZING = "deserialization";
    private static final String NODE_PREFIX = "node:";
    private static final String ROUND_TRIP_SUFFIX = ".rtt";
    private static final String ONE_WAY_SUFFIX = ".oneway";

    private static final String UPDATE_MESSAGE_NAME = "ClusterCommunicationManager-update";
    private static final long GOSSIP_INTERVAL_MILLIS = 1000;
    private static final long TOMBSTONE_EXPIRATION_MILLIS = 1000 * 60;

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
            .register(KryoNamespaces.API)
            .register(Subscription.class)
            .register(MessageSubject.class)
            .register(LogicalTimestamp.class)
            .register(WallClockTimestamp.class)
            .build());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    private ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected MessagingService messagingService;

    private final AtomicLong logicalTime = new AtomicLong();
    private ScheduledExecutorService gossipExecutor;
    private NodeId localNodeId;
    private final Map<NodeId, Long> updateTimes = Maps.newConcurrentMap();
    private final Map<MessageSubject, Map<NodeId, Subscription>> subjectSubscriptions = Maps.newConcurrentMap();
    private final Map<MessageSubject, SubscriberIterator> subjectIterators = Maps.newConcurrentMap();
    private final BiFunction<Endpoint, byte[], byte[]> gossipConsumer = (ep, payload) -> {
        update(SERIALIZER.decode(payload));
        return SERIALIZER.encode(null);
    };

    @Activate
    public void activate() {
        localNodeId = clusterService.getLocalNode().id();
        gossipExecutor = Executors.newSingleThreadScheduledExecutor(
                groupedThreads("onos/cluster", "onos-cluster-executor-%d"));
        gossipExecutor.scheduleAtFixedRate(
                this::gossip,
                GOSSIP_INTERVAL_MILLIS,
                GOSSIP_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
        gossipExecutor.scheduleAtFixedRate(
                this::purgeTombstones,
                TOMBSTONE_EXPIRATION_MILLIS,
                TOMBSTONE_EXPIRATION_MILLIS,
                TimeUnit.MILLISECONDS);
        messagingService.registerHandler(UPDATE_MESSAGE_NAME, gossipConsumer, gossipExecutor);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        messagingService.unregisterHandler(UPDATE_MESSAGE_NAME);
        gossipExecutor.shutdown();
        log.info("Stopped");
    }

    @Override
    public <M> void broadcast(M message,
                              MessageSubject subject,
                              Function<M, byte[]> encoder) {
        checkPermission(CLUSTER_WRITE);
        Collection<? extends NodeId> subscribers = getSubscriberNodes(subject);
        if (subscribers != null) {
            multicast(message,
                    subject,
                    encoder,
                    subscribers.stream()
                            .filter(node -> !Objects.equal(node, localNodeId))
                            .collect(Collectors.toSet()));
        }
    }

    @Override
    public <M> void broadcastIncludeSelf(M message,
                                         MessageSubject subject,
                                         Function<M, byte[]> encoder) {
        checkPermission(CLUSTER_WRITE);
        Collection<? extends NodeId> subscribers = getSubscriberNodes(subject);
        if (subscribers != null) {
            multicast(message,
                    subject,
                    encoder,
                    ImmutableSet.copyOf(subscribers));
        }
    }

    @Override
    public <M> CompletableFuture<Void> unicast(M message,
                                               MessageSubject subject,
                                               Function<M, byte[]> encoder) {
        NodeId nodeId = getNextNodeId(subject);
        if (nodeId != null) {
            return unicast(message, subject, encoder, nodeId);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public <M> CompletableFuture<Void> unicast(M message,
                                               MessageSubject subject,
                                               Function<M, byte[]> encoder,
                                               NodeId toNodeId) {
        checkPermission(CLUSTER_WRITE);
        try {
            byte[] payload = new ClusterMessage(
                    localNodeId,
                    subject,
                    timeFunction(encoder, subjectMeteringAgent, SERIALIZING).apply(message)
                    ).getBytes();
            return doUnicast(subject, payload, toNodeId);
        } catch (Exception e) {
            return Tools.exceptionalFuture(e);
        }
    }

    @Override
    public <M> void multicast(M message,
                              MessageSubject subject,
                              Function<M, byte[]> encoder) {
        checkPermission(CLUSTER_WRITE);
        byte[] payload = new ClusterMessage(
                localNodeId,
                subject,
                timeFunction(encoder, subjectMeteringAgent, SERIALIZING).apply(message))
                .getBytes();
        Collection<? extends NodeId> subscribers = getSubscriberNodes(subject);
        if (subscribers != null) {
            subscribers.forEach(nodeId -> doUnicast(subject, payload, nodeId));
        }
    }

    @Override
    public <M> void multicast(M message,
                              MessageSubject subject,
                              Function<M, byte[]> encoder,
                              Set<NodeId> nodes) {
        checkPermission(CLUSTER_WRITE);
        byte[] payload = new ClusterMessage(
                localNodeId,
                subject,
                timeFunction(encoder, subjectMeteringAgent, SERIALIZING).apply(message))
                .getBytes();
        nodes.forEach(nodeId -> doUnicast(subject, payload, nodeId));
    }

    @Override
    public <M, R> CompletableFuture<R> sendAndReceive(M message,
                                                      MessageSubject subject,
                                                      Function<M, byte[]> encoder,
                                                      Function<byte[], R> decoder) {
        NodeId nodeId = getNextNodeId(subject);
        if (nodeId == null) {
            return Tools.exceptionalFuture(new MessagingException.NoRemoteHandler());
        }
        return sendAndReceive(message, subject, encoder, decoder, nodeId);
    }

    @Override
    public <M, R> CompletableFuture<R> sendAndReceive(M message,
                                                      MessageSubject subject,
                                                      Function<M, byte[]> encoder,
                                                      Function<byte[], R> decoder,
                                                      NodeId toNodeId) {
        checkPermission(CLUSTER_WRITE);
        try {
            ClusterMessage envelope = new ClusterMessage(
                    clusterService.getLocalNode().id(),
                    subject,
                    timeFunction(encoder, subjectMeteringAgent, SERIALIZING).
                            apply(message));
            return sendAndReceive(subject, envelope.getBytes(), toNodeId).
                    thenApply(bytes -> timeFunction(decoder, subjectMeteringAgent, DESERIALIZING).apply(bytes));
        } catch (Exception e) {
            return Tools.exceptionalFuture(e);
        }
    }

    private CompletableFuture<Void> doUnicast(MessageSubject subject, byte[] payload, NodeId toNodeId) {
        ControllerNode node = clusterService.getNode(toNodeId);
        checkArgument(node != null, "Unknown nodeId: %s", toNodeId);
        Endpoint nodeEp = new Endpoint(node.ip(), node.tcpPort());
        MeteringAgent.Context context = subjectMeteringAgent.startTimer(subject.toString() + ONE_WAY_SUFFIX);
        return messagingService.sendAsync(nodeEp, subject.value(), payload).whenComplete((r, e) -> context.stop(e));
    }

    private CompletableFuture<byte[]> sendAndReceive(MessageSubject subject, byte[] payload, NodeId toNodeId) {
        ControllerNode node = clusterService.getNode(toNodeId);
        checkArgument(node != null, "Unknown nodeId: %s", toNodeId);
        Endpoint nodeEp = new Endpoint(node.ip(), node.tcpPort());
        MeteringAgent.Context epContext = endpointMeteringAgent.
                startTimer(NODE_PREFIX + toNodeId.toString() + ROUND_TRIP_SUFFIX);
        MeteringAgent.Context subjectContext = subjectMeteringAgent.
                startTimer(subject.toString() + ROUND_TRIP_SUFFIX);
        return messagingService.sendAndReceive(nodeEp, subject.value(), payload).
                whenComplete((bytes, throwable) -> {
                    subjectContext.stop(throwable);
                    epContext.stop(throwable);
                });
    }

    /**
     * Returns the set of nodes with subscribers for the given message subject.
     *
     * @param subject the subject for which to return a set of nodes
     * @return a set of nodes with subscribers for the given subject
     */
    private Collection<? extends NodeId> getSubscriberNodes(MessageSubject subject) {
        Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.get(subject);
        if (nodeSubscriptions == null) {
            return null;
        }
        return nodeSubscriptions.values()
                .stream()
                .filter(s -> clusterService.getState(s.nodeId()).isActive() && !s.isTombstone())
                .map(s -> s.nodeId())
                .collect(Collectors.toList());
    }

    /**
     * Returns the next node ID for the given message subject.
     *
     * @param subject the subject for which to return the next node ID
     * @return the next node ID for the given message subject
     */
    private NodeId getNextNodeId(MessageSubject subject) {
        SubscriberIterator iterator = subjectIterators.get(subject);
        return iterator != null && iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Resets the iterator for the given message subject.
     *
     * @param subject the subject for which to reset the iterator
     */
    private synchronized void setSubscriberIterator(MessageSubject subject) {
        Collection<? extends NodeId> subscriberNodes = getSubscriberNodes(subject);
        if (subscriberNodes != null && !subscriberNodes.isEmpty()) {
            subjectIterators.put(subject, new SubscriberIterator(subscriberNodes));
        } else {
            subjectIterators.remove(subject);
        }
    }

    /**
     * Registers the node as a subscriber for the given subject.
     *
     * @param subject the subject for which to register the node as a subscriber
     */
    private synchronized void registerSubscriber(MessageSubject subject) {
        Map<NodeId, Subscription> nodeSubscriptions =
                subjectSubscriptions.computeIfAbsent(subject, s -> Maps.newConcurrentMap());
        Subscription subscription = new Subscription(
                localNodeId,
                subject,
                new LogicalTimestamp(logicalTime.incrementAndGet()));
        nodeSubscriptions.put(localNodeId, subscription);
        updateNodes();
    }

    /**
     * Unregisters the node as a subscriber for the given subject.
     *
     * @param subject the subject for which to unregister the node as a subscriber
     */
    private synchronized void unregisterSubscriber(MessageSubject subject) {
        Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.get(subject);
        if (nodeSubscriptions != null) {
            Subscription subscription = nodeSubscriptions.get(localNodeId);
            if (subscription != null) {
                nodeSubscriptions.put(localNodeId, subscription.asTombstone());
                updateNodes();
            }
        }
    }

    @Override
    public void addSubscriber(MessageSubject subject,
                              ClusterMessageHandler subscriber,
                              ExecutorService executor) {
        checkPermission(CLUSTER_WRITE);
        messagingService.registerHandler(subject.value(),
                new InternalClusterMessageHandler(subscriber),
                executor);
        registerSubscriber(subject);
    }

    @Override
    public void removeSubscriber(MessageSubject subject) {
        checkPermission(CLUSTER_WRITE);
        messagingService.unregisterHandler(subject.value());
        unregisterSubscriber(subject);
    }

    @Override
    public <M, R> void addSubscriber(MessageSubject subject,
            Function<byte[], M> decoder,
            Function<M, R> handler,
            Function<R, byte[]> encoder,
            Executor executor) {
        checkPermission(CLUSTER_WRITE);
        messagingService.registerHandler(subject.value(),
                new InternalMessageResponder<M, R>(decoder, encoder, m -> {
                    CompletableFuture<R> responseFuture = new CompletableFuture<>();
                    executor.execute(() -> {
                        try {
                            responseFuture.complete(handler.apply(m));
                        } catch (Exception e) {
                            responseFuture.completeExceptionally(e);
                        }
                    });
                    return responseFuture;
                }));
        registerSubscriber(subject);
    }

    @Override
    public <M, R> void addSubscriber(MessageSubject subject,
            Function<byte[], M> decoder,
            Function<M, CompletableFuture<R>> handler,
            Function<R, byte[]> encoder) {
        checkPermission(CLUSTER_WRITE);
        messagingService.registerHandler(subject.value(),
                new InternalMessageResponder<>(decoder, encoder, handler));
        registerSubscriber(subject);
    }

    @Override
    public <M> void addSubscriber(MessageSubject subject,
            Function<byte[], M> decoder,
            Consumer<M> handler,
            Executor executor) {
        checkPermission(CLUSTER_WRITE);
        messagingService.registerHandler(subject.value(),
                new InternalMessageConsumer<>(decoder, handler),
                executor);
        registerSubscriber(subject);
    }

    /**
     * Performs the timed function, returning the value it would while timing the operation.
     *
     * @param timedFunction the function to be timed
     * @param meter the metering agent to be used to time the function
     * @param opName the opname to be used when starting the meter
     * @param <A> The param type of the function
     * @param <B> The return type of the function
     * @return the value returned by the timed function
     */
    private <A, B> Function<A, B> timeFunction(Function<A, B> timedFunction,
                                               MeteringAgent meter, String opName) {
        checkNotNull(timedFunction);
        checkNotNull(meter);
        checkNotNull(opName);
        return new Function<A, B>() {
            @Override
            public B apply(A a) {
                final MeteringAgent.Context context = meter.startTimer(opName);
                B result = null;
                try {
                    result = timedFunction.apply(a);
                    context.stop(null);
                    return result;
                } catch (Exception e) {
                    context.stop(e);
                    Throwables.propagate(e);
                    return null;
                }
            }
        };
    }

    /**
     * Handles a collection of subscription updates received via the gossip protocol.
     *
     * @param subscriptions a collection of subscriptions provided by the sender
     */
    private void update(Collection<Subscription> subscriptions) {
        for (Subscription subscription : subscriptions) {
            Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.computeIfAbsent(
                    subscription.subject(), s -> Maps.newConcurrentMap());
            Subscription existingSubscription = nodeSubscriptions.get(subscription.nodeId());
            if (existingSubscription == null
                    || existingSubscription.logicalTimestamp().isOlderThan(subscription.logicalTimestamp())) {
                nodeSubscriptions.put(subscription.nodeId(), subscription);
                setSubscriberIterator(subscription.subject());
            }
        }
    }

    /**
     * Sends a gossip message to an active peer.
     */
    private void gossip() {
        List<ControllerNode> nodes = clusterService.getNodes()
                .stream()
                .filter(node -> !localNodeId.equals(node.id()))
                .filter(node -> clusterService.getState(node.id()).isActive())
                .collect(Collectors.toList());

        if (!nodes.isEmpty()) {
            Collections.shuffle(nodes);
            ControllerNode node = nodes.get(0);
            updateNode(node);
        }
    }

    /**
     * Updates all active peers with a given subscription.
     */
    private void updateNodes() {
        clusterService.getNodes()
                .stream()
                .filter(node -> !localNodeId.equals(node.id()))
                .forEach(this::updateNode);
    }

    /**
     * Sends an update to the given node.
     *
     * @param node the node to which to send the update
     */
    private void updateNode(ControllerNode node) {
        long updateTime = System.currentTimeMillis();
        long lastUpdateTime = updateTimes.getOrDefault(node.id(), 0L);

        Collection<Subscription> subscriptions = new ArrayList<>();
        subjectSubscriptions.values().forEach(ns -> ns.values()
                .stream()
                .filter(subscription -> subscription.timestamp().unixTimestamp() > lastUpdateTime)
                .forEach(subscriptions::add));

        Endpoint ep = new Endpoint(node.ip(), node.tcpPort());
        messagingService.sendAndReceive(ep, UPDATE_MESSAGE_NAME, SERIALIZER.encode(subscriptions))
                .whenComplete((result, error) -> {
                    if (error == null) {
                        updateTimes.put(node.id(), updateTime);
                    }
                });
    }

    /**
     * Purges tombstones from the subscription list.
     */
    private void purgeTombstones() {
        long minTombstoneTime = clusterService.getNodes().stream()
                .map(node -> updateTimes.getOrDefault(node.id(), 0L))
                .reduce(Math::min)
                .orElse(0L);
        for (Map<NodeId, Subscription> nodeSubscriptions : subjectSubscriptions.values()) {
            Iterator<Map.Entry<NodeId, Subscription>> nodeSubscriptionIterator =
                    nodeSubscriptions.entrySet().iterator();
            while (nodeSubscriptionIterator.hasNext()) {
                Subscription subscription = nodeSubscriptionIterator.next().getValue();
                if (subscription.isTombstone() && subscription.timestamp().unixTimestamp() < minTombstoneTime) {
                    nodeSubscriptionIterator.remove();
                }
            }
        }
    }

    private class InternalClusterMessageHandler implements BiFunction<Endpoint, byte[], byte[]> {
        private ClusterMessageHandler handler;

        public InternalClusterMessageHandler(ClusterMessageHandler handler) {
            this.handler = handler;
        }

        @Override
        public byte[] apply(Endpoint sender, byte[] bytes) {
            ClusterMessage message = ClusterMessage.fromBytes(bytes);
            handler.handle(message);
            return message.response();
        }
    }

    private class InternalMessageResponder<M, R> implements BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> {
        private final Function<byte[], M> decoder;
        private final Function<R, byte[]> encoder;
        private final Function<M, CompletableFuture<R>> handler;

        public InternalMessageResponder(Function<byte[], M> decoder,
                                        Function<R, byte[]> encoder,
                                        Function<M, CompletableFuture<R>> handler) {
            this.decoder = decoder;
            this.encoder = encoder;
            this.handler = handler;
        }

        @Override
        public CompletableFuture<byte[]> apply(Endpoint sender, byte[] bytes) {
            return handler.apply(timeFunction(decoder, subjectMeteringAgent, DESERIALIZING).
                    apply(ClusterMessage.fromBytes(bytes).payload())).
                    thenApply(m -> timeFunction(encoder, subjectMeteringAgent, SERIALIZING).apply(m));
        }
    }

    private class InternalMessageConsumer<M> implements BiConsumer<Endpoint, byte[]> {
        private final Function<byte[], M> decoder;
        private final Consumer<M> consumer;

        public InternalMessageConsumer(Function<byte[], M> decoder, Consumer<M> consumer) {
            this.decoder = decoder;
            this.consumer = consumer;
        }

        @Override
        public void accept(Endpoint sender, byte[] bytes) {
            consumer.accept(timeFunction(decoder, subjectMeteringAgent, DESERIALIZING).
                    apply(ClusterMessage.fromBytes(bytes).payload()));
        }
    }

    /**
     * Subscriber iterator that iterates subscribers in a loop.
     */
    private class SubscriberIterator implements Iterator<NodeId> {
        private final AtomicInteger counter = new AtomicInteger();
        private final NodeId[] subscribers;
        private final int length;

        SubscriberIterator(Collection<? extends NodeId> subscribers) {
            this.length = subscribers.size();
            this.subscribers = subscribers.toArray(new NodeId[length]);
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public NodeId next() {
            return subscribers[counter.incrementAndGet() % length];
        }
    }
}
