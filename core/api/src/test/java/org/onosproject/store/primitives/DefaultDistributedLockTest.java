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
package org.onosproject.store.primitives;

import org.junit.Test;
import org.onosproject.store.service.DistributedPrimitive;
import org.onosproject.store.service.TestLock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Default distributed lock test.
 */
public class DefaultDistributedLockTest {

    /**
     * Simple test testing that lock method calls are properly passed through to the underlying asynchronous lock.
     */
    @Test
    public void testLockBehavior() throws Throwable {
        DistributedLock lock = new DefaultDistributedLock(new TestLock(),
                DistributedPrimitive.DEFAULT_OPERTATION_TIMEOUT_MILLIS);
        lock.lock();
        assertFalse(lock.tryLock());
        lock.unlock();
        assertTrue(lock.tryLock());
        lock.unlock();
    }

}
