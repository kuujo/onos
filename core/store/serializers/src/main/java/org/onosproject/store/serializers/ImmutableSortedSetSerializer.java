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
package org.onosproject.store.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableSortedSet;

/**
* Kryo Serializer for {@link ImmutableSortedSet}.
*/
public class ImmutableSortedSetSerializer extends Serializer<ImmutableSortedSet<?>> {

    /**
     * Creates {@link ImmutableSortedSet} serializer instance.
     */
    public ImmutableSortedSetSerializer() {
        // non-null, immutable
        super(false, true);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableSortedSet<?> object) {
        output.writeInt(object.size());
        for (Object e : object) {
            kryo.writeClassAndObject(output, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ImmutableSortedSet<?> read(Kryo kryo, Input input,
                                Class<ImmutableSortedSet<?>> type) {
        final int size = input.readInt();
        switch (size) {
        case 0:
            return ImmutableSortedSet.of();
        case 1:
            return ImmutableSortedSet.of((Comparable) kryo.readClassAndObject(input));
        default:
            Comparable[] elms = new Comparable[size];
            for (int i = 0; i < size; ++i) {
                elms[i] = (Comparable) kryo.readClassAndObject(input);
            }
            return ImmutableSortedSet.copyOf(elms);
        }
    }
}
