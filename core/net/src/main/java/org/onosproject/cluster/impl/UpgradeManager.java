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

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.ClusterEvent;
import org.onosproject.cluster.ClusterEventListener;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.UpgradeEvent;
import org.onosproject.cluster.UpgradeEventListener;
import org.onosproject.cluster.UpgradeService;
import org.onosproject.cluster.UpgradeStore;
import org.onosproject.cluster.UpgradeStoreDelegate;
import org.onosproject.core.Version;
import org.onosproject.core.VersionService;
import org.onosproject.event.AbstractListenerManager;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.AtomicValue;
import org.onosproject.store.service.AtomicValueEvent;
import org.onosproject.store.service.AtomicValueEventListener;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade service implementation.
 */
@Component(immediate = true)
@Service
public class UpgradeManager
        extends AbstractListenerManager<UpgradeEvent, UpgradeEventListener>
        implements UpgradeService {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC, UpgradeStatus.class);

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final UpgradeStoreDelegate delegate = new InternalStoreDelegate();

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected VersionService versionService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected UpgradeStore store;

    private AtomicValue<UpgradeStatus> statusValue;
    private UpgradeStatus currentStatus;

    private final ClusterEventListener clusterListener = event -> clusterChanged(event);
    private final AtomicValueEventListener<UpgradeStatus> statusListener = event -> statusChanged(event);

    @Activate
    public void activate() {
        store.setDelegate(delegate);

        statusValue = storageService.<UpgradeStatus>atomicValueBuilder()
                .withName("upgrade-status")
                .withSerializer(SERIALIZER)
                .build()
                .asAtomicValue();
        statusValue.addListener(statusListener);
        clusterService.addListener(clusterListener);

        Version localVersion = versionService.version();

        // Initialize the current upgrade status to the local version.
        currentStatus = new UpgradeStatus(localVersion, UpgradeStatus.State.COMPLETE);

        if (!statusValue.compareAndSet(null, currentStatus)) {
            currentStatus = statusValue.get();
            if (currentStatus.state() == UpgradeStatus.State.COMPLETE) {
                if (!canUpgrade(currentStatus.version(), localVersion)) {
                    throw new IllegalStateException(
                            String.format("Cannot start node with version %s: Cannot upgrade version %s to %s",
                                    localVersion, currentStatus.version(), localVersion));
                } else {

                }
            } else if (currentStatus.state() == UpgradeStatus.State.IN_PROGRESS) {

            }
        }

        if (!store.setCurrentVersion(localVersion)) {

        }

        // Attempt to set the upgrade status for the first time. If the status is not null, read the status
        // and ensure this node's version is consistent with the current cluster upgrade status.
        if (!statusValue.compareAndSet(null, currentStatus)) {
            currentStatus = statusValue.get();
            if (localVersion.isGreaterThan(currentStatus.version())) {
                if (currentStatus.state() == UpgradeStatus.State.COMPLETE) {
                    initializeUpgrade(currentStatus.version(), localVersion);
                } else if (currentStatus.state() == UpgradeStatus.State.IN_PROGRESS) {
                    log.error("Cannot start node with version {}: Upgrade to version {} already in progress!",
                            localVersion, currentStatus.version());
                    throw new IllegalStateException("Upgrade to " + currentStatus.version() + " already in progress!");
                }
            }
        }
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        store.unsetDelegate(delegate);
        clusterService.removeListener(clusterListener);
        statusValue.removeListener(statusListener);
        log.info("Stopped");
    }

    @Override
    public Version getVersion() {
        return store.getCurrentVersion();
    }

    /**
     * Initializes an upgrade to the given version.
     *
     * @param upgradeVersion the upgraded cluster version
     */
    private void initializeUpgrade(Version upgradeVersion) {
        if (store.beginUpgrade(upgradeVersion)) {
            log.info("Initialized upgrade from version {} to {}", store.getCurrentVersion(), upgradeVersion);
        }
    }

    /**
     * Returns a boolean indicating whether the cluster can be upgraded between the two given versions.
     * <p>
     * The cluster can be upgraded from the current version to the given version if the upgrade represents
     * a single minor version bump or the upgrade is the first minor version of the next major version.
     *
     * @param currentVersion the current cluster version
     * @param upgradeVersion the upgraded cluster version
     * @return whether the given current version can be upgraded to the given upgrade version
     */
    private boolean canUpgrade(Version currentVersion, Version upgradeVersion) {
        // The upgrade version must be greater than the current version.
        return upgradeVersion.isGreaterThan(currentVersion)
                // The upgrade's major version is equal to the current major version...
                && (upgradeVersion.major() == upgradeVersion.major()
                // and upgrade's minor version is equal to the current minor version or minor plus one...
                && (upgradeVersion.minor() == currentVersion.minor()
                || upgradeVersion.minor() == currentVersion.minor() + 1))
                // or upgrade's major version is equal to the current major version plus one
                || (upgradeVersion.major() == currentVersion.major() + 1
                // and upgrade's minor version is 0
                && upgradeVersion.minor() == 0);
    }

    /**
     * Handles a status changed event.
     */
    private void statusChanged(AtomicValueEvent<UpgradeStatus> event) {
        currentStatus = event.newValue();
        if (event.oldValue() == null || event.oldValue().state() != event.newValue().state()) {
            switch (currentStatus.state()) {
                case IN_PROGRESS:
                    post(new UpgradeEvent(UpgradeEvent.Type.UPGRADE_STARTED, currentStatus.version()));
                    break;
                case COMPLETE:
                    post(new UpgradeEvent(UpgradeEvent.Type.UPGRADE_COMPLETE, currentStatus.version()));
                    break;
            }
        }
    }

    /**
     * Called when a cluster change event occurs.
     */
    private void clusterChanged(ClusterEvent event) {
        // If no upgrade is in progress, ignore the cluster event.
        if (currentStatus.state() != UpgradeStatus.State.IN_PROGRESS) {
            return;
        }

        // If the event is not an INSTANCE_READY event, ignore it.
        if (event.type() != ClusterEvent.Type.INSTANCE_READY) {
            return;
        }

        // Determine whether all nodes have been upgraded to the new version and are in the READY state.
        boolean allUpgraded = clusterService.getNodes().stream()
                .map(node -> Pair.of(clusterService.getState(node.id()), clusterService.getVersion(node.id())))
                .allMatch(pair -> pair.getLeft() == ControllerNode.State.READY
                        && Objects.equals(pair.getRight(), currentStatus.version()));

        // If all nodes have finished upgrade, update the status value. Trigger upgrade events
        // when the atomic value event arrives back from the store.
        if (allUpgraded) {
            UpgradeStatus newStatus = new UpgradeStatus(currentStatus.version(), UpgradeStatus.State.COMPLETE);
            statusValue.compareAndSet(currentStatus, newStatus);
        }
    }

    private class InternalStoreDelegate implements UpgradeStoreDelegate {
        @Override
        public void notify(UpgradeEvent event) {
            post(event);
        }
    }
}
