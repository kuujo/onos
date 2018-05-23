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
package org.onosproject.store.primitives.impl;

import java.io.File;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Collections2;
import io.atomix.cluster.MemberId;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.NodeId;
import org.onosproject.cluster.Partition;
import org.onosproject.cluster.PartitionId;
import org.onosproject.core.Version;
import org.onosproject.store.cluster.messaging.ClusterCommunicationService;
import org.onosproject.store.service.PartitionInfo;
import org.onosproject.store.service.Serializer;

/**
 * Storage partition.
 */
public class StoragePartition implements Managed<StoragePartition> {

    static final String PARTITIONS_DIR =
            System.getProperty("karaf.data") + "/db/partitions/";

    protected final AtomicBoolean isOpened = new AtomicBoolean(false);
    protected final ClusterCommunicationService clusterCommunicator;
    protected final ClusterService clusterService;
    protected Partition partition;
    protected NodeId localNodeId;
    protected StoragePartitionServer server;
    protected StoragePartitionClient client;

    public StoragePartition(
            Partition partition,
            ClusterCommunicationService clusterCommunicator,
            ClusterService clusterService) {
        this.partition = partition;
        this.clusterCommunicator = clusterCommunicator;
        this.clusterService = clusterService;
        this.localNodeId = clusterService.getLocalNode().id();
    }

    /**
     * Returns the partition client instance.
     * @return client
     */
    public StoragePartitionClient client() {
        return client;
    }

    @Override
    public CompletableFuture<Void> open() {
        if (partition.getMembers().contains(localNodeId)) {
            return openServer()
                    .thenCompose(v -> openClient())
                    .thenAccept(v -> isOpened.set(true))
                    .thenApply(v -> null);
        }
        return openClient()
                .thenAccept(v -> isOpened.set(true))
                .thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> close() {
        // We do not explicitly close the server and instead let the cluster
        // deal with this as an unclean exit.
        return closeClient();
    }

    /**
     * Deletes the partition.
     *
     * @return future to be completed once the partition has been deleted
     */
    public CompletableFuture<Void> delete() {
        return closeServer().thenCompose(v -> closeClient()).thenRun(() -> deleteServer());
    }

    /**
     * Returns the partition data folder.
     *
     * @return the partition data folder
     */
    public File getDataFolder() {
        return new File(PARTITIONS_DIR + partition.getId());
    }

    /**
     * Returns the partition name.
     *
     * @return the partition name
     */
    public String getName() {
        return partition.getId().toString();
    }

    /**
     * Returns the identifier of the {@link Partition partition} associated with this instance.
     *
     * @return partition identifier
     */
    public PartitionId getId() {
        return partition.getId();
    }

    /**
     * Returns the partition version.
     *
     * @return the partition version
     */
    public Version getVersion() {
        return partition.getVersion();
    }

    /**
     * Returns the identifiers of partition members.
     * @return partition member instance ids
     */
    public Collection<NodeId> getMembers() {
        return partition.getMembers();
    }

    /**
     * Returns the {@link MemberId identifiers} of partition members.
     * @return partition member identifiers
     */
    public Collection<MemberId> getMemberIds() {
        return Collections2.transform(getMembers(), n -> MemberId.from(n.id()));
    }

    /**
     * Attempts to rejoin the partition.
     * @return future that is completed after the operation is complete
     */
    protected CompletableFuture<Void> openServer() {
        StoragePartitionServer server = new StoragePartitionServer(
            this,
            MemberId.from(localNodeId.id()),
            clusterService,
            clusterCommunicator);
        return server.open().thenRun(() -> this.server = server);
    }

    /**
     * Attempts to join the partition as a new member.
     * @return future that is completed after the operation is complete
     */
    private CompletableFuture<Void> joinCluster() {
        Set<NodeId> otherMembers = partition.getMembers()
                 .stream()
                 .filter(nodeId -> !nodeId.equals(localNodeId))
                 .collect(Collectors.toSet());
        StoragePartitionServer server = new StoragePartitionServer(this,
                MemberId.from(localNodeId.id()),
                clusterService,
                clusterCommunicator);
        return server.join(Collections2.transform(otherMembers, n -> MemberId.from(n.id())))
                .thenRun(() -> this.server = server);
    }

    private CompletableFuture<StoragePartitionClient> openClient() {
        client = new StoragePartitionClient(this,
                MemberId.from(localNodeId.id()),
                new RaftClientCommunicator(
                        String.format("partition-%s-%s", partition.getId(), partition.getVersion()),
                        Serializer.using(StorageNamespaces.RAFT_PROTOCOL),
                        clusterCommunicator));
        return client.open().thenApply(v -> client);
    }

    /**
     * Closes the partition server if it was previously opened.
     * @return future that is completed when the operation completes
     */
    public CompletableFuture<Void> leaveCluster() {
        return server != null
                ? server.closeAndExit().thenRun(() -> server.delete())
                : CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isOpen() {
        return isOpened.get();
    }

    private CompletableFuture<Void> closeServer() {
        if (server != null) {
            return server.close();
        }
        return CompletableFuture.completedFuture(null);
    }

    private void deleteServer() {
        if (server != null) {
            server.delete();
        }
    }

    private CompletableFuture<Void> closeClient() {
        if (client != null) {
            return client.close();
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Returns the partition information if this partition is locally managed i.e.
     * this node is a active member of the partition.
     * @return partition info
     */
    public Optional<PartitionInfo> info() {
        return server != null && server.isOpen() ? Optional.of(server.info()) : Optional.empty();
    }

    /**
     * Process updates to partitions and handles joining or leaving a partition.
     * @param newValue new Partition
     */
    void onUpdate(Partition newValue) {
        boolean wasPresent = partition.getMembers().contains(localNodeId);
        boolean isPresent = newValue.getMembers().contains(localNodeId);
        this.partition = newValue;
        if ((wasPresent && isPresent) || (!wasPresent && !isPresent)) {
            // no action needed
            return;
        }
        // Only need to do action if our membership changed
        if (wasPresent) {
            leaveCluster();
        } else {
            joinCluster();
        }
    }
}
