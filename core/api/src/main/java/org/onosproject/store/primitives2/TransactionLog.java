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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Transaction log.
 */
public class TransactionLog<T> {
    private final List<T> entries;

    public TransactionLog(List<T> entries) {
        this.entries = entries;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public int size() {
        return entries.size();
    }

    public List<T> entries() {
        return entries;
    }

    public TransactionLog<T> filter(Predicate<T> filter) {
        return new TransactionLog<>(entries.stream().filter(filter).collect(Collectors.toList()));
    }

    public <U> TransactionLog<U> map(Function<T, U> mapper) {
        return new TransactionLog<>(entries.stream().map(mapper).collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("entries", entries)
                .toString();
    }
}
