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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import org.junit.Test;
import org.onlab.util.Tools;
import org.onosproject.cluster.PartitionId;
import org.onosproject.store.primitives.DistributedPrimitiveCreator;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMap;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapService;
import org.onosproject.store.primitives.resources.impl.AtomixTestBase;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.MapEvent;
import org.onosproject.store.service.MapEventListener;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.Versioned;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AtomixConsistentMap}.
 */
public class PartitionedConsistentMapTest extends AtomixTestBase<AtomixConsistentMap> {

    @Override
    protected RaftService createService() {
        return new AtomixConsistentMapService();
    }

    @Override
    protected AtomixConsistentMap createPrimitive(RaftProxy proxy) {
        return new AtomixConsistentMap(proxy);
    }

    @SuppressWarnings("unchecked")
    private <K, V> AsyncConsistentMap<K, V> createPartitionedMap(String name) {
        Map<PartitionId, DistributedPrimitiveCreator> partitions = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            PartitionId partitionId = PartitionId.from(i);
            partitions.put(partitionId, new DistributedPrimitiveCreatorAdapter() {
                @Override
                public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(String name, Serializer serializer) {
                    return (AsyncConsistentMap<K, V>) newPrimitive(name + "-" + partitionId);
                }
            });
        }

        DistributedPrimitiveCreator primitiveCreator = new FederatedDistributedPrimitiveCreator(partitions);
        return primitiveCreator.newAsyncConsistentMap(name, Serializer.using(KryoNamespaces.BASIC));
    }

    /**
     * Tests various basic map operations.
     */
    @Test
    public void testBasicMapOperations() throws Throwable {
        AsyncConsistentMap<String, String> map = createPartitionedMap("partitioned-map-test");

        String rawFooValue = "Hello foo!";
        String rawBarValue = "Hello bar!";

        map.isEmpty().thenAccept(result -> {
            assertTrue(result);
        }).join();

        map.put("foo", rawFooValue).thenAccept(result -> {
            assertNull(result);
        }).join();

        map.size().thenAccept(result -> {
            assertTrue(result == 1);
        }).join();

        map.isEmpty().thenAccept(result -> {
            assertFalse(result);
        }).join();

        map.putIfAbsent("foo", "Hello foo again!").thenAccept(result -> {
            assertNotNull(result);
            assertTrue(Objects.equals(Versioned.valueOrElse(result, null), rawFooValue));
        }).join();

        map.putIfAbsent("bar", rawBarValue).thenAccept(result -> {
            assertNull(result);
        }).join();

        map.size().thenAccept(result -> {
            assertTrue(result == 2);
        }).join();

        map.keySet().thenAccept(result -> {
            assertTrue(result.size() == 2);
            assertTrue(result.containsAll(Sets.newHashSet("foo", "bar")));
        }).join();

        map.values().thenAccept(result -> {
            assertTrue(result.size() == 2);
            List<String> rawValues =
                    result.stream().map(v -> v.value()).collect(Collectors.toList());
            assertTrue(rawValues.contains("Hello foo!"));
            assertTrue(rawValues.contains("Hello bar!"));
        }).join();

        map.entrySet().thenAccept(result -> {
            assertTrue(result.size() == 2);
            // TODO: check entries
        }).join();

        map.get("foo").thenAccept(result -> {
            assertTrue(Objects.equals(Versioned.valueOrElse(result, null), rawFooValue));
        }).join();

        map.remove("foo").thenAccept(result -> {
            assertTrue(Objects.equals(Versioned.valueOrElse(result, null), rawFooValue));
        }).join();

        map.containsKey("foo").thenAccept(result -> {
            assertFalse(result);
        }).join();

        map.get("foo").thenAccept(result -> {
            assertNull(result);
        }).join();

        map.get("bar").thenAccept(result -> {
            assertNotNull(result);
            assertTrue(Objects.equals(Versioned.valueOrElse(result, null), rawBarValue));
        }).join();

        map.containsKey("bar").thenAccept(result -> {
            assertTrue(result);
        }).join();

        map.size().thenAccept(result -> {
            assertTrue(result == 1);
        }).join();

        map.containsValue(rawBarValue).thenAccept(result -> {
            assertTrue(result);
        }).join();

        map.containsValue(rawFooValue).thenAccept(result -> {
            assertFalse(result);
        }).join();

        map.replace("bar", "Goodbye bar!").thenAccept(result -> {
            assertNotNull(result);
            assertTrue(Objects.equals(Versioned.valueOrElse(result, null), rawBarValue));
        }).join();

        map.replace("foo", "Goodbye foo!").thenAccept(result -> {
            assertNull(result);
        }).join();

        // try replace_if_value_match for a non-existent key
        map.replace("foo", "Goodbye foo!", rawFooValue).thenAccept(result -> {
            assertFalse(result);
        }).join();

        map.replace("bar", "Goodbye bar!", rawBarValue).thenAccept(result -> {
            assertTrue(result);
        }).join();

        map.replace("bar", "Goodbye bar!", rawBarValue).thenAccept(result -> {
            assertFalse(result);
        }).join();

        Versioned<String> barValue = map.get("bar").join();
        map.replace("bar", barValue.version(), "Goodbye bar!").thenAccept(result -> {
            assertTrue(result);
        }).join();

        map.replace("bar", barValue.version(), rawBarValue).thenAccept(result -> {
            assertFalse(result);
        }).join();

        map.clear().join();

        map.size().thenAccept(result -> {
            assertTrue(result == 0);
        }).join();
    }

    @Test
    public void testMapListeners() throws Throwable {
        final String value1 ="value1";
        final String value2 = "value2";
        final String value3 = "value3";

        AsyncConsistentMap<String, String> map = createPartitionedMap("testMapListenerMap");
        TestMapEventListener listener = new TestMapEventListener();

        // add listener; insert new value into map and verify an INSERT event is received.
        map.addListener(listener).thenCompose(v -> map.put("foo", value1)).join();
        MapEvent<String, String> event = listener.event();
        assertNotNull(event);
        assertEquals(MapEvent.Type.INSERT, event.type());
        assertTrue(Objects.equals(value1, event.newValue().value()));

        // remove listener and verify listener is not notified.
        map.removeListener(listener).thenCompose(v -> map.put("foo", value2)).join();
        assertFalse(listener.eventReceived());

        // add the listener back and verify UPDATE events are received correctly
        map.addListener(listener).thenCompose(v -> map.put("foo", value3)).join();
        event = listener.event();
        assertNotNull(event);
        assertEquals(MapEvent.Type.UPDATE, event.type());
        assertTrue(Objects.equals(value3, event.newValue().value()));

        // perform a non-state changing operation and verify no events are received.
        map.putIfAbsent("foo", value1).join();
        assertFalse(listener.eventReceived());

        // verify REMOVE events are received correctly.
        map.remove("foo").join();
        event = listener.event();
        assertNotNull(event);
        assertEquals(MapEvent.Type.REMOVE, event.type());
        assertTrue(Objects.equals(value3, event.oldValue().value()));

        // verify compute methods also generate events.
        map.computeIf("foo", v -> v == null, (k, v) -> value1).join();
        event = listener.event();
        assertNotNull(event);
        assertEquals(MapEvent.Type.INSERT, event.type());
        assertTrue(Objects.equals(value1, event.newValue().value()));

        map.compute("foo", (k, v) -> value2).join();
        event = listener.event();
        assertNotNull(event);
        assertEquals(MapEvent.Type.UPDATE, event.type());
        assertTrue(Objects.equals(value2, event.newValue().value()));

        map.computeIf("foo", v -> Objects.equals(v, value2), (k, v) -> null).join();
        event = listener.event();
        assertNotNull(event);
        assertEquals(MapEvent.Type.REMOVE, event.type());
        assertTrue(Objects.equals(value2, event.oldValue().value()));

        map.removeListener(listener).join();
    }

    private static class TestMapEventListener implements MapEventListener<String, String> {

        private final BlockingQueue<MapEvent<String, String>> queue = new ArrayBlockingQueue<>(1);

        @Override
        public void event(MapEvent<String, String> event) {
            try {
                queue.put(event);
            } catch (InterruptedException e) {
                Throwables.propagate(e);
            }
        }

        public boolean eventReceived() {
            return !queue.isEmpty();
        }

        public MapEvent<String, String> event() throws InterruptedException {
            return queue.take();
        }
    }
}
