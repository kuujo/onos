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
package org.onosproject.issu;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onlab.packet.IpAddress;
import org.onosproject.cluster.MembershipService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.intentsync.IntentSynchronizationService;
import org.onosproject.net.Host;
import org.onosproject.net.host.HostService;
import org.onosproject.net.intent.HostToHostIntent;
import org.onosproject.net.intent.Key;
import org.onosproject.store.service.StorageService;
import org.onosproject.upgrade.UpgradeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(immediate = true)
@Service
public class SampleManager implements SampleService {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    private CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    private UpgradeService upgradeService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    private HostService hostService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    private StorageService storageService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    private IntentSynchronizationService intentSynchronizationService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    private MembershipService membershipService;

    private HostToHostIntent intent;
    private ApplicationId appId;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication("org.onosproject.issu");
        if (hostService.getHostCount() >= 4) {
            if (upgradeService.isLocalUpgraded() && membershipService.getMembers().size() == 1) {
                // TODO: check if all upgraded and then delete old flows
                // intentSynchronizationService.removeIntentsByAppId(appId);

                // INSTALL INTENT AFTER UPGRADE
                Host h1, h2;
                h1 = hostService.getHostsByIp(IpAddress.valueOf("10.0.0.3")).iterator().next();
                h2 = hostService.getHostsByIp(IpAddress.valueOf("10.0.0.4")).iterator().next();

                intent = HostToHostIntent.builder()
                        .appId(appId)
                        .key(Key.of(h1.id().toString() + "-" + h2.id().toString(), appId))
                        .one(h1.id())
                        .two(h2.id())
                        .build();

                log.warn("Installing {}", h1.id().toString() + "-" + h2.id().toString());
                intentSynchronizationService.submit(intent);
            } else if (!upgradeService.isLocalUpgraded()) {
                // INSTALL INTENT BEFORE UPGRADE
                Host h1, h2;
                h1 = hostService.getHostsByIp(IpAddress.valueOf("10.0.0.1")).iterator().next();
                h2 = hostService.getHostsByIp(IpAddress.valueOf("10.0.0.2")).iterator().next();

                intent = HostToHostIntent.builder()
                        .appId(appId)
                        .key(Key.of(h1.id().toString() + "-" + h2.id().toString(), appId))
                        .one(h1.id())
                        .two(h2.id())
                        .build();

                log.warn("Installing {}", h1.id().toString() + "-" + h2.id().toString());
                intentSynchronizationService.submit(intent);
            }
        }

        log.info("Sample Service Started");
    }

    @Deactivate
    protected void deactivate() {
        intentSynchronizationService.removeIntentsByAppId(appId);
        log.info("Sample Service Stopped");
    }
}
