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
package org.onosproject.distributedprimitives.cli;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.onosproject.cli.AbstractShellCommand;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.NodeId;
import org.onosproject.store.primitives.DistributedLock;
import org.onosproject.store.service.StorageService;

/**
 * Distributed lock test command.
 */
@Command(scope = "onos", name = "lock-test",
        description = "DistributedLock test cli fixture")
public class DistributedLockTestCommand extends AbstractShellCommand {
    @Argument(index = 0, name = "name",
            description = "lock name",
            required = true, multiValued = false)
    String name = null;

    @Argument(index = 1, name = "operation",
            description = "operation",
            required = true, multiValued = false)
    String operation = null;


    @Argument(index = 2, name = "topic",
            description = "topic name",
            required = false, multiValued = false)
    String topic = null;

    DistributedLock lock;

    @Override
    protected void execute() {
        StorageService storageService = get(StorageService.class);
        ClusterService clusterService = get(ClusterService.class);
        NodeId localNodeId = clusterService.getLocalNode().id();
        lock = storageService.lockBuilder()
                .withName(name)
                .build();
        if ("lock".equals(operation)) {
            lock.lock();
        } else if ("unlock".equals(operation)) {
            lock.unlock();
        }
    }

}
