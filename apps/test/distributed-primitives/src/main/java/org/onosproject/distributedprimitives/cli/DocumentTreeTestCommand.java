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
package org.onosproject.distributedprimitives.cli;

import java.util.Arrays;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.onosproject.cli.AbstractShellCommand;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.DocumentPath;
import org.onosproject.store.service.DocumentTree;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.StorageService;
import org.onosproject.store.service.Versioned;

/**
 * CLI command to manipulate a document tree.
 */
@Command(scope = "onos", name = "doc-tree-test",
        description = "Manipulate a document tree")
public class DocumentTreeTestCommand extends AbstractShellCommand {

    @Argument(index = 0, name = "name",
            description = "map name",
            required = true, multiValued = false)
    String name = null;

    @Argument(index = 1, name = "operation",
            description = "operation name",
            required = true, multiValued = false)
    String operation = null;

    @Argument(index = 2, name = "path",
            description = "first arg",
            required = false, multiValued = false)
    String arg1 = null;

    @Argument(index = 3, name = "value1",
            description = "second arg",
            required = false, multiValued = false)
    String arg2 = null;

    @Argument(index = 4, name = "value2",
            description = "third arg",
            required = false, multiValued = false)
    String arg3 = null;

    DocumentTree<String> tree;

    @Override
    protected void execute() {
        StorageService storageService = get(StorageService.class);
        tree = storageService.<String>documentTreeBuilder()
                .withName(name)
                .withSerializer(Serializer.using(KryoNamespaces.BASIC))
                .build()
                .asDocumentTree();

        if ("create".equals(operation)) {
            print("%b", tree.create(path(arg1), arg2));
        } else if ("createRecursive".equals(operation)) {
            print("%b", tree.createRecursive(path(arg1), arg2));
        } else if ("get".equals(operation)) {
            print(tree.get(path(arg1)));
        } else if ("set".equals(operation)) {
            print(tree.set(path(arg1), arg2));
        } else if ("removeNode".equals(operation)) {
            print(tree.removeNode(path(arg1)));
        } else if ("replace".equals(operation)) {
            try {
                long version = Long.parseLong(arg3);
                print("%b", tree.replace(path(arg1), arg2, version));
            } catch (NumberFormatException e) {
                print("%b", tree.replace(path(arg1), arg2, arg3));
            }
        }
    }

    private DocumentPath path(String path) {
        // The Karaf CLI is not friendly to the pipe delimiter, so we use a simple dotted path.
        return DocumentPath.from(Arrays.asList(path.split("\\.")));
    }

    private void print(Versioned<String> value) {
        if (value == null) {
            print("null");
        } else {
            print("%s", value.value());
        }
    }
}
