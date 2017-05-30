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
package org.onosproject.store.primitives.resources.impl;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

/**
 * Long commands.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class AtomixCounterCommands {

    private AtomixCounterCommands() {
    }

    /**
     * Abstract value command.
     */
    public static abstract class ValueCommand<V> implements Command<V>, CatalystSerializable {
        @Override
        public CompactionMode compaction() {
            return CompactionMode.SNAPSHOT;
        }

        @Override
        public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
        }

        @Override
        public void readObject(BufferInput<?> buffer, Serializer serializer) {
        }
    }

    /**
     * Abstract value query.
     */
    public static abstract class ValueQuery<V> implements Query<V>, CatalystSerializable {
        private ConsistencyLevel consistency;

        protected ValueQuery() {
        }

        protected ValueQuery(Query.ConsistencyLevel consistency) {
            this.consistency = consistency;
        }

        @Override
        public void writeObject(BufferOutput<?> output, Serializer serializer) {
            if (consistency != null) {
                output.writeByte(consistency.ordinal());
            } else {
                output.writeByte(-1);
            }
        }

        @Override
        public void readObject(BufferInput<?> input, Serializer serializer) {
            int ordinal = input.readByte();
            if (ordinal != -1) {
                consistency = ConsistencyLevel.values()[ordinal];
            }
        }
    }

    /**
     * Get query.
     */
    public static class Get extends ValueQuery<Long> {
        public Get() {
        }

        public Get(ConsistencyLevel consistency) {
            super(consistency);
        }
    }

    /**
     * Set command.
     */
    public static class Set extends ValueCommand<Void> {
        private Long value;

        public Set() {
        }

        public Set(Long value) {
            this.value = value;
        }

        /**
         * Returns the command value.
         *
         * @return The command value.
         */
        public Long value() {
            return value;
        }

        @Override
        public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
            serializer.writeObject(value, buffer);
        }

        @Override
        public void readObject(BufferInput<?> buffer, Serializer serializer) {
            value = serializer.readObject(buffer);
        }

        @Override
        public String toString() {
            return String.format("%s[value=%s]", getClass().getSimpleName(), value);
        }
    }

    /**
     * Compare and set command.
     */
    public static class CompareAndSet extends ValueCommand<Boolean> {
        private Long expect;
        private Long update;

        public CompareAndSet() {
        }

        public CompareAndSet(Long expect, Long update) {
            this.expect = expect;
            this.update = update;
        }

        /**
         * Returns the expected value.
         *
         * @return The expected value.
         */
        public Long expect() {
            return expect;
        }

        /**
         * Returns the updated value.
         *
         * @return The updated value.
         */
        public Long update() {
            return update;
        }

        @Override
        public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
            serializer.writeObject(expect, buffer);
            serializer.writeObject(update, buffer);
        }

        @Override
        public void readObject(BufferInput<?> buffer, Serializer serializer) {
            expect = serializer.readObject(buffer);
            update = serializer.readObject(buffer);
        }

        @Override
        public String toString() {
            return String.format("%s[expect=%s, update=%s]", getClass().getSimpleName(), expect, update);
        }
    }

    /**
     * Abstract long command.
     */
    public static abstract class LongCommand<V> implements Command<V>, CatalystSerializable {

        protected LongCommand() {
        }

        @Override
        public CompactionMode compaction() {
            return CompactionMode.SNAPSHOT;
        }

        @Override
        public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
        }

        @Override
        public void readObject(BufferInput<?> buffer, Serializer serializer) {
        }
    }

    /**
     * Increment and get command.
     */
    public static class IncrementAndGet extends LongCommand<Long> {
    }

    /**
     * Get and increment command.
     */
    public static class GetAndIncrement extends LongCommand<Long> {
    }

    /**
     * Delta command.
     */
    public static abstract class DeltaCommand extends LongCommand<Long> {
        private long delta;

        public DeltaCommand() {
        }

        public DeltaCommand(long delta) {
            this.delta = delta;
        }

        /**
         * Returns the delta.
         *
         * @return The delta.
         */
        public long delta() {
            return delta;
        }

        @Override
        public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
            buffer.writeLong(delta);
        }

        @Override
        public void readObject(BufferInput<?> buffer, Serializer serializer) {
            delta = buffer.readLong();
        }
    }

    /**
     * Get and add command.
     */
    public static class GetAndAdd extends DeltaCommand {
        public GetAndAdd() {
        }

        public GetAndAdd(long delta) {
            super(delta);
        }
    }

    /**
     * Add and get command.
     */
    public static class AddAndGet extends DeltaCommand {
        public AddAndGet() {
        }

        public AddAndGet(long delta) {
            super(delta);
        }
    }

    /**
     * Value command type resolver.
     */
    public static class TypeResolver implements SerializableTypeResolver {
        @Override
        public void resolve(SerializerRegistry registry) {
            registry.register(CompareAndSet.class, -110);
            registry.register(Get.class, -111);
            registry.register(Set.class, -113);
            registry.register(IncrementAndGet.class, -114);
            registry.register(GetAndIncrement.class, -116);
            registry.register(AddAndGet.class, -118);
            registry.register(GetAndAdd.class, -119);
        }
    }

}