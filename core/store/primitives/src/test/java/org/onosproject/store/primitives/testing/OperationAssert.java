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
package org.onosproject.store.primitives.testing;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Operation assertion.
 */
public abstract class OperationAssert<T extends OperationAssert<T, U>, U> {
    private U returnValue;

    public OperationAssert(U returnValue) {
        this.returnValue = returnValue;
    }

    @SuppressWarnings("unchecked")
    public T returns(U value) {
        assertEquals(value, returnValue);
        return (T) this;
    }

    public T returnsNull() {
        return returns(null);
    }

    @SuppressWarnings("unchecked")
    public T returnMatches(Function<U, Boolean> callback) {
        assertTrue(callback.apply(returnValue));
        return (T) this;
    }
}
