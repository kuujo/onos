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
package org.onosproject.store.primitives.impl;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import org.junit.Ignore;
import org.junit.Test;
import org.onlab.util.HexString;
import org.onosproject.store.primitives.DistributedPrimitiveCreator;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMap;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapService;
import org.onosproject.store.primitives.resources.impl.AtomixTestBase;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.AsyncAtomicCounter;
import org.onosproject.store.service.AsyncAtomicCounterMap;
import org.onosproject.store.service.AsyncAtomicIdGenerator;
import org.onosproject.store.service.AsyncAtomicValue;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.AsyncConsistentMultimap;
import org.onosproject.store.service.AsyncConsistentTreeMap;
import org.onosproject.store.service.AsyncDistributedSet;
import org.onosproject.store.service.AsyncDocumentTree;
import org.onosproject.store.service.AsyncLeaderElector;
import org.onosproject.store.service.DocumentPath;
import org.onosproject.store.service.IllegalDocumentModificationException;
import org.onosproject.store.service.NoSuchDocumentPathException;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.Versioned;
import org.onosproject.store.service.WorkQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class PartitionedAsyncDocumentTreeTest extends AtomixTestBase<AtomixConsistentMap> {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC);

    private final DistributedPrimitiveCreator primitiveCreator = new TestPrimitiveCreator();

    @Override
    protected RaftService createService() {
        return new AtomixConsistentMapService();
    }

    @Override
    protected AtomixConsistentMap createPrimitive(RaftProxy proxy) {
        return new AtomixConsistentMap(proxy);
    }

    private AsyncDocumentTree<String> createTree() {
        return new PartitionedAsyncDocumentTree<>(UUID.randomUUID().toString(), SERIALIZER, primitiveCreator);
    }

    @Test
    public void testTreeConstructor() {
        AsyncDocumentTree<String> tree = createTree();
        assertEquals(tree.root(), path(tree.name()));
    }

    @Test
    public void testCreateNodeAtRoot() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertFalse(tree.create(path(tree.name(), "a"), "baz").join());
    }

    @Test
    public void testCreateNodeAtNonRoot() {
        AsyncDocumentTree<String> tree = createTree();
        tree.create(path(tree.name(), "a"), "bar");
        assertTrue(tree.create(path(tree.name(), "a", "b"), "baz").join());
    }

    @Test
    public void testCreateRecursive() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.createRecursive(path(tree.name(), "a", "b", "c"), "bar").join());
        assertEquals(tree.get(path(tree.name(), "a", "b", "c")).join().value(), "bar");
        assertNull(tree.get(path(tree.name(), "a", "b")).join().value());
        assertNull(tree.get(path(tree.name(), "a")).join().value());
    }

    @Test(expected = IllegalDocumentModificationException.class)
    public void testCreateRecursiveRoot() throws Throwable {
        AsyncDocumentTree<String> tree = createTree();
        try {
            tree.createRecursive(path(tree.name()), "bar").join();
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = IllegalDocumentModificationException.class)
    public void testCreateNodeFailure() throws Throwable {
        AsyncDocumentTree<String> tree = createTree();
        try {
            tree.create(path(tree.name(), "a", "b"), "bar").join();
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testGetRootValue() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertTrue(tree.create(path(tree.name(), "a", "b"), "baz").join());
        Versioned<String> root = tree.get(path(tree.name())).join();
        assertNotNull(root);
        assertNull(root.value());
    }

    @Test
    public void testGetInnerNode() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertTrue(tree.create(path(tree.name(), "a", "b"), "baz").join());
        Versioned<String> nodeValue = tree.get(path(tree.name(), "a")).join();
        assertNotNull(nodeValue);
        assertEquals("bar", nodeValue.value());
    }

    @Test
    public void testGetLeafNode() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertTrue(tree.create(path(tree.name(), "a", "b"), "baz").join());
        Versioned<String> nodeValue = tree.get(path(tree.name(), "a", "b")).join();
        assertNotNull(nodeValue);
        assertEquals("baz", nodeValue.value());
    }

    @Test
    public void getMissingNode() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertTrue(tree.create(path(tree.name(), "a", "b"), "baz").join());
        assertNull(tree.get(path(tree.name(), "x")).join());
        assertNull(tree.get(path(tree.name(), "a", "x")).join());
        assertNull(tree.get(path(tree.name(), "a", "b", "x")).join());
    }

    @Test
    public void testGetChildren() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertTrue(tree.create(path(tree.name(), "a", "b"), "alpha").join());
        assertTrue(tree.create(path(tree.name(), "a", "c"), "beta").join());
        assertEquals(2, tree.getChildren(path(tree.name(), "a")).join().size());
        assertEquals(0, tree.getChildren(path(tree.name(), "a", "b")).join().size());
    }

    @Test(expected = NoSuchDocumentPathException.class)
    public void testGetChildrenFailure() throws Throwable {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        try {
            tree.getChildren(path(tree.name(), "a", "b")).join();
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = IllegalDocumentModificationException.class)
    public void testSetRootFailure() throws Throwable {
        AsyncDocumentTree<String> tree = createTree();
        try {
            tree.set(tree.root(), "bar").join();
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testSet() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertNull(tree.set(path(tree.name(), "a", "b"), "alpha").join());
        assertEquals("alpha", tree.set(path(tree.name(), "a", "b"), "beta").join().value());
        assertEquals("beta", tree.get(path(tree.name(), "a", "b")).join().value());
    }

    @Test(expected = IllegalDocumentModificationException.class)
    public void testSetInvalidNode() throws Throwable {
        AsyncDocumentTree<String> tree = createTree();
        try {
            tree.set(path(tree.name(), "a", "b"), "alpha").join();
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testReplaceWithVersion() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertTrue(tree.create(path(tree.name(), "a", "b"), "alpha").join());
        Versioned<String> value = tree.get(path(tree.name(), "a", "b")).join();
        assertTrue(tree.replace(path(tree.name(), "a", "b"), "beta", value.version()).join());
        assertFalse(tree.replace(path(tree.name(), "a", "b"), "beta", value.version()).join());
        assertFalse(tree.replace(path(tree.name(), "x"), "beta", 1).join());
    }

    @Test
    @Ignore
    public void testReplaceWithValue() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertTrue(tree.create(path(tree.name(), "a", "b"), "alpha").join());
        assertTrue(tree.replace(path(tree.name(), "a", "b"), "beta", "alpha").join());
        assertFalse(tree.replace(path(tree.name(), "a", "b"), "beta", "alpha").join());
        assertFalse(tree.replace(path(tree.name(), "x"), "beta", "bar").join());
        assertTrue(tree.replace(path(tree.name(), "x"), "beta", null).join());
    }

    @Test(expected = IllegalDocumentModificationException.class)
    public void testRemoveRoot() throws Throwable {
        AsyncDocumentTree<String> tree = createTree();
        try {
            tree.removeNode(tree.root());
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testRemove() {
        AsyncDocumentTree<String> tree = createTree();
        assertTrue(tree.create(path(tree.name(), "a"), "bar").join());
        assertTrue(tree.create(path(tree.name(), "a", "b"), "alpha").join());
        assertEquals("alpha", tree.removeNode(path(tree.name(), "a", "b")).join().value());
        assertEquals(0, tree.getChildren(path(tree.name(), "a")).join().size());
    }

    @Test(expected = NoSuchDocumentPathException.class)
    public void testRemoveInvalidNode() throws Throwable {
        AsyncDocumentTree<String> tree = createTree();
        try {
            tree.removeNode(path(tree.name(), "a")).join();
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    private static DocumentPath path(String... path) {
        return DocumentPath.from(path);
    }

    private class TestPrimitiveCreator implements DistributedPrimitiveCreator {
        @Override
        public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(String name, Serializer serializer) {
            return DistributedPrimitives.newTranscodingMap(newPrimitive(name),
                    key -> HexString.toHexString(serializer.encode(key)),
                    string -> serializer.decode(HexString.fromHexString(string)),
                    value -> value == null ? null : serializer.encode(value),
                    bytes -> serializer.decode(bytes));
        }

        @Override
        public <V> AsyncConsistentTreeMap<V> newAsyncConsistentTreeMap(String name, Serializer serializer) {
            return null;
        }

        @Override
        public <K, V> AsyncConsistentMultimap<K, V> newAsyncConsistentSetMultimap(String name, Serializer serializer) {
            return null;
        }

        @Override
        public <K> AsyncAtomicCounterMap<K> newAsyncAtomicCounterMap(String name, Serializer serializer) {
            return null;
        }

        @Override
        public AsyncAtomicCounter newAsyncCounter(String name) {
            return null;
        }

        @Override
        public AsyncAtomicIdGenerator newAsyncIdGenerator(String name) {
            return null;
        }

        @Override
        public <V> AsyncAtomicValue<V> newAsyncAtomicValue(String name, Serializer serializer) {
            return null;
        }

        @Override
        public <E> AsyncDistributedSet<E> newAsyncDistributedSet(String name, Serializer serializer) {
            return null;
        }

        @Override
        public AsyncLeaderElector newAsyncLeaderElector(String name) {
            return null;
        }

        @Override
        public <E> WorkQueue<E> newWorkQueue(String name, Serializer serializer) {
            return null;
        }

        @Override
        public <V> AsyncDocumentTree<V> newAsyncDocumentTree(String name, Serializer serializer) {
            return null;
        }

        @Override
        public Set<String> getAsyncConsistentMapNames() {
            return null;
        }

        @Override
        public Set<String> getAsyncAtomicCounterNames() {
            return null;
        }

        @Override
        public Set<String> getWorkQueueNames() {
            return null;
        }
    }
}
