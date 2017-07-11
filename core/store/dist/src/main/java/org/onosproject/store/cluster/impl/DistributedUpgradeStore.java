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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.UpgradeEvent;
import org.onosproject.cluster.UpgradeStore;
import org.onosproject.cluster.UpgradeStoreDelegate;
import org.onosproject.core.Version;
import org.onosproject.store.AbstractStore;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.AtomicValue;
import org.onosproject.store.service.AtomicValueEvent;
import org.onosproject.store.service.AtomicValueEventListener;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed upgrade store.
 */
@Service
@Component(immediate = true)
public class DistributedUpgradeStore
        extends AbstractStore<UpgradeEvent, UpgradeStoreDelegate>
        implements UpgradeStore {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC, UpgradeMetadata.class);

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    protected AtomicValue<UpgradeMetadata> metadataValue;

    private final AtomicValueEventListener<UpgradeMetadata> metadataListener = event -> metadataChanged(event);

    @Activate
    public void activate() {
        metadataValue.addListener(metadataListener);
    }

    @Deactivate
    public void deactivate() {
        metadataValue.removeListener(metadataListener);
    }

    @Override
    public Version getCurrentVersion() {
        UpgradeMetadata metadata = metadataValue.get();
        return metadata == null ? null : metadata.state() == UpgradeMetadata.State.COMPLETE
                ? metadata.upgradeVersion()
                : metadata.currentVersion();
    }

    @Override
    public boolean setCurrentVersion(Version currentVersion) {
        UpgradeMetadata metadata = new UpgradeMetadata(currentVersion, null, UpgradeMetadata.State.STABLE);
        return metadataValue.compareAndSet(null, metadata);
    }

    /**
     * Handles an upgrade metadata change event.
     *
     * @param event the metadata change event
     */
    protected void metadataChanged(AtomicValueEvent<UpgradeMetadata> event) {
        if (event.oldValue() == null) {
            return;
        }

        switch (event.newValue().state()) {
            case STABLE:
                break;
            case IN_PROGRESS:
                notifyDelegate(new UpgradeEvent(UpgradeEvent.Type.UPGRADE_STARTED, event.newValue().upgradeVersion()));
                break;
            case COMPLETE:
                notifyDelegate(new UpgradeEvent(UpgradeEvent.Type.UPGRADE_COMPLETE, event.newValue().upgradeVersion()));
                break;
        }
    }

    @Override
    public boolean beginUpgrade(Version upgradeVersion) {
        UpgradeMetadata metadata = metadataValue.get();
        if (metadata == null || metadata.state() != UpgradeMetadata.State.STABLE) {
            return false;
        }

        UpgradeMetadata newMetadata = new UpgradeMetadata(
                metadata.currentVersion(),
                upgradeVersion,
                UpgradeMetadata.State.IN_PROGRESS);
        return metadataValue.compareAndSet(metadata, newMetadata);
    }

    @Override
    public boolean completeUpgrade() {
        UpgradeMetadata metadata = metadataValue.get();
        if (metadata == null || metadata.state() != UpgradeMetadata.State.IN_PROGRESS) {
            return false;
        }

        UpgradeMetadata newMetadata = new UpgradeMetadata(
                metadata.currentVersion(),
                metadata.upgradeVersion(),
                UpgradeMetadata.State.COMPLETE);
        return metadataValue.compareAndSet(metadata, newMetadata);
    }

    @Override
    public boolean rollbackUpgrade() {
        UpgradeMetadata metadata = metadataValue.get();
        if (metadata == null || metadata.state() != UpgradeMetadata.State.IN_PROGRESS) {
            return false;
        }

        UpgradeMetadata newMetadata = new UpgradeMetadata(
                metadata.currentVersion(),
                null,
                UpgradeMetadata.State.STABLE);
        return metadataValue.compareAndSet(metadata, newMetadata);
    }
}
