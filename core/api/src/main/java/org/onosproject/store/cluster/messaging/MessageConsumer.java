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
package org.onosproject.store.cluster.messaging;

import org.onosproject.core.Version;

/**
 * Message consumer that consumes messages with source information.
 */
public interface MessageConsumer<T> {

    /**
     * Consumes a message with source information.
     *
     * @param endpoint the source endpoint
     * @param message the message
     * @param version the source version
     */
    void consume(Endpoint endpoint, T message, Version version);

}
