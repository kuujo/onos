/*
 * Copyright 2015-present Open Networking Laboratory
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

import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.onosproject.cli.AbstractShellCommand;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.CommitStatus;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.StorageService;
import org.onosproject.store.service.TransactionContext;
import org.onosproject.store.service.TransactionalMap;

/**
 * CLI command to get a value associated with a specific key in a transactional map.
 */
@Command(scope = "onos", name = "transactional-map-test-get",
        description = "Get a value associated with a specific key in a transactional map")
public class TransactionalMapTestGetCommand extends AbstractShellCommand {

    @Argument(index = 0, name = "numKeys",
            description = "Number of keys to get",
            required = true, multiValued = false)
    private int numKeys = 1;

    String prefix = "Key";
    String mapName = "Test-Map";
    Serializer serializer = Serializer.using(KryoNamespaces.BASIC);

    @Override
    protected void execute() {
        StorageService storageService = get(StorageService.class);
        CommitStatus status = null;
        while (status != CommitStatus.SUCCESS) {
            TransactionContext context = storageService.transactionContextBuilder().build();
            context.begin();
            try {
                TransactionalMap<String, String> map = context.getTransactionalMap(mapName, serializer);
                Set<String> values = Sets.newHashSet();
                for (int i = 1; i <= numKeys; i++) {
                    String key = prefix + i;
                    String value = map.get(key);
                    if (value != null) {
                        values.add(value);
                    }
                }

                if ((status = context.commit().join()) == CommitStatus.SUCCESS) {
                    if (values.size() == 1) {
                        print("single");
                    } else if (values.isEmpty()) {
                        print("none");
                    } else {
                        print("multiple (" + Joiner.on(',').join(values) + ")");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                print("Aborting transaction: %s", e.getMessage());
                context.abort();
                break;
            }
        }
    }
}