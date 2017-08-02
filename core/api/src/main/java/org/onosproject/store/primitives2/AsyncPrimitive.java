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
package org.onosproject.store.primitives2;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous primitive.
 */
public interface AsyncPrimitive<T extends SynchronousPrimitive> extends DistributedPrimitive {

    /**
     * Destroys the primitive.
     *
     * @return future to be completed once the primitive's state has been destroyed
     */
    CompletableFuture<Void> destroy();

    /**
     * Closes the primitive.
     *
     * @return future to be completed once the primitive has been closed
     */
    CompletableFuture<Void> close();

    /**
     * Returns a synchronous instance of the primitive.
     *
     * @return a synchronous instance of the primitive
     */
    T sync();

}
