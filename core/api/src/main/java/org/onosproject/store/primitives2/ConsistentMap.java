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

import org.onosproject.store.service.Versioned;

/**
 *
 */
public interface ConsistentMap<K, V> extends SynchronousPrimitive<AsyncConsistentMap<K, V>> {

    boolean containsKey(K key);

    Versioned<V> get(K key);

    Versioned<V> put(K key, V value);

    int size();

    void clear();

}
