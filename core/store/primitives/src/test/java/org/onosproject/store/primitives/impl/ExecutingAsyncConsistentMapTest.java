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

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.onosproject.store.service.AsyncConsistentMap;
import org.onosproject.store.service.Versioned;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * {@link ExecutingAsyncConsistentMap} test.
 */
public class ExecutingAsyncConsistentMapTest {

    @Test
    public void testExecutingAsyncConsistentMapMethods() throws Exception {
        AsyncConsistentMap<String, String> executingMap = new ExecutingAsyncConsistentMap<>(
                new TestAsyncConsistentMap<>(), MoreExecutors.directExecutor(), MoreExecutors.directExecutor());

        assertEquals(new Integer(0), executingMap.size().join());
        assertTrue(executingMap.isEmpty().join());

        executingMap.put("foo", "Hello world!").join();
        executingMap.put("bar", "Hello world again!").join();

        assertNull(executingMap.get("baz").join());
        assertEquals("Hello world!", executingMap.get("foo").join().value());
        assertEquals("Hello world again!", executingMap.get("bar").join().value());
        assertFalse(executingMap.containsKey("baz").join());
        assertTrue(executingMap.containsKey("foo").join());
        assertTrue(executingMap.containsValue("Hello world!").join());
        assertEquals(new Integer(2), executingMap.size().join());
        assertFalse(executingMap.isEmpty().join());

        executingMap.clear().join();

        assertEquals(new Integer(0), executingMap.size().join());
        assertTrue(executingMap.isEmpty().join());
        assertNotNull(executingMap.getOrDefault("baz", "foo").join());

        executingMap.put("foo", "Hello world!").join();
        assertEquals("Hello world!", executingMap.putAndGet("foo", "Hello world again!").join().value());
        assertEquals("Hello world again!", executingMap.remove("foo").join().value());

        assertNull(executingMap.put("foo", "Hello world!").join());
        assertTrue(executingMap.remove("foo", "Hello world!").join());
        executingMap.put("bar", "Hello world!").join();

        Versioned<String> versioned = executingMap.get("bar").join();
        assertFalse(executingMap.remove("bar", 0).join());
        assertTrue(executingMap.remove("bar", versioned.version()).join());

        executingMap.put("foo", "Hello world!").join();
        assertEquals(1, executingMap.keySet().join().size());
        assertEquals("foo", executingMap.keySet().join().iterator().next());
        assertEquals(1, executingMap.values().join().size());
        assertEquals("Hello world!", executingMap.values().join().iterator().next().value());
        assertEquals(1, executingMap.entrySet().join().size());

        assertEquals("Hello world again!",
                executingMap.putIfAbsent("baz", "Hello world again!").join().value());
    }

}
