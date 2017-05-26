/*
 * Copyright 2014-present Open Networking Laboratory
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
package org.onlab.util;

import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ReferenceResolver;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.StreamFactory;
import com.esotericsoftware.kryo.factories.SerializerFactory;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.IdentityObjectIntMap;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.esotericsoftware.kryo.util.ObjectMap;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.esotericsoftware.kryo.util.Util.className;
import static com.esotericsoftware.minlog.Log.TRACE;
import static com.esotericsoftware.minlog.Log.trace;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Pool of Kryo instances, with classes pre-registered.
 */
//@ThreadSafe
public final class KryoNamespace implements KryoFactory, KryoPool {

    /**
     * Default buffer size used for serialization.
     *
     * @see #serialize(Object)
     */
    public static final int DEFAULT_BUFFER_SIZE = 4096;
    public static final int MAX_BUFFER_SIZE = 100 * 1000 * 1000;

    /**
     * ID to use if this KryoNamespace does not define registration id.
     */
    public static final int FLOATING_ID = -1;

    /**
     * Smallest ID free to use for user defined registrations.
     */
    public static final int INITIAL_ID = 16;

    private static final String NO_NAME = "(no name)";

    private static final Logger log = getLogger(KryoNamespace.class);



    private final KryoPool pool = new KryoPool.Builder(this)
                                        .softReferences()
                                        .build();

    private final ImmutableList<RegistrationBlock> registeredBlocks;
    private final ImmutableList<RegistrationBlock> registeredAbstracts;
    private final ImmutableMap<Class<?>, Serializer<?>> registeredDefaults;

    private final boolean registrationRequired;
    private final String friendlyName;

    /**
     * KryoNamespace builder.
     */
    //@NotThreadSafe
    public static final class Builder {

        private int blockHeadId = INITIAL_ID;
        private List<Pair<Class<?>, Serializer<?>>> types = new ArrayList<>();
        private List<Pair<Class<?>, Serializer<?>>> abstracts = new ArrayList<>();
        private Map<Class<?>, Serializer<?>> defaults = new HashMap<>();
        private List<RegistrationBlock> blocks = new ArrayList<>();
        private List<RegistrationBlock> abstractBlocks = new ArrayList<>();
        private boolean registrationRequired = true;

        /**
         * Builds a {@link KryoNamespace} instance.
         *
         * @return KryoNamespace
         */
        public KryoNamespace build() {
            return build(NO_NAME);
        }

        /**
         * Builds a {@link KryoNamespace} instance.
         *
         * @param friendlyName friendly name for the namespace
         * @return KryoNamespace
         */
        public KryoNamespace build(String friendlyName) {
            if (!abstracts.isEmpty()) {
                abstractBlocks.add(new RegistrationBlock(this.blockHeadId, abstracts));
            }
            if (!types.isEmpty()) {
                int id = this.blockHeadId;
                if (id != FLOATING_ID) {
                    id += abstracts.size();
                }
                blocks.add(new RegistrationBlock(id, types));
            }
            return new KryoNamespace(blocks, abstractBlocks, defaults, registrationRequired, friendlyName).populate(1);
        }

        /**
         * Sets the next Kryo registration Id for following register entries.
         *
         * @param id Kryo registration Id
         * @return this
         *
         * @see Kryo#register(Class, Serializer, int)
         */
        public Builder nextId(final int id) {
            if (!types.isEmpty() || !abstracts.isEmpty()) {
                if (id != FLOATING_ID && id < blockHeadId + types.size() + abstracts.size()) {
                    if (log.isWarnEnabled()) {
                        log.warn("requested nextId {} could potentially overlap " +
                                 "with existing registrations {}+{} ",
                                 id, blockHeadId, types.size() + abstracts.size(), new RuntimeException());
                    }
                }

                int abstractCount = abstracts.size();
                if (abstractCount > 0) {
                    blocks.add(new RegistrationBlock(this.blockHeadId, abstracts));
                    abstracts = new ArrayList<>();
                }

                if (!types.isEmpty()) {
                    int headId = this.blockHeadId;
                    if (headId != FLOATING_ID) {
                        headId += abstractCount;
                    }
                    blocks.add(new RegistrationBlock(headId, types));
                    types = new ArrayList<>();
                }
            }
            this.blockHeadId = id;
            return this;
        }

        /**
         * Registers an abstract serializer.
         *
         * @param serializer the abstract serializer to register
         * @param classes the classes for which to register the abstract serializer
         * @return this
         */
        public Builder registerAbstract(Serializer<?> serializer, Class<?>... classes) {
            for (Class<?> clazz : classes) {
                abstracts.add(Pair.of(clazz, serializer));
            }
            return this;
        }

        /**
         * Registers a default serializer.
         *
         * @param serializer the default serializer to register
         * @param classes the classes for which to register the default serializer
         * @return this
         */
        public Builder registerDefault(Serializer<?> serializer, Class<?>... classes) {
            for (Class<?> clazz : classes) {
                defaults.put(clazz, serializer);
            }
            return this;
        }

        /**
         * Registers classes to be serialized using Kryo default serializer.
         *
         * @param expectedTypes list of classes
         * @return this
         */
        public Builder register(final Class<?>... expectedTypes) {
            for (Class<?> clazz : expectedTypes) {
                types.add(Pair.of(clazz, null));
            }
            return this;
        }

        /**
         * Registers a class and it's serializer.
         *
         * @param classes list of classes to register
         * @param serializer serializer to use for the class
         * @return this
         */
        public Builder register(Serializer<?> serializer, final Class<?>... classes) {
            for (Class<?> clazz : classes) {
                types.add(Pair.of(clazz, serializer));
            }
            return this;
        }

        private Builder registerAbstract(RegistrationBlock block) {
            return register(block, abstractBlocks);
        }

        private Builder register(RegistrationBlock block) {
            return register(block, blocks);
        }

        private Builder register(RegistrationBlock block, List<RegistrationBlock> blocks) {
            if (block.begin() != FLOATING_ID) {
                // flush pending types
                nextId(block.begin());
                blocks.add(block);
                nextId(block.begin() + block.types().size());
            } else {
                // flush pending types
                final int addedBlockBegin = blockHeadId + abstracts.size() + types.size();
                nextId(addedBlockBegin);
                blocks.add(new RegistrationBlock(addedBlockBegin, block.types()));
                nextId(addedBlockBegin + block.types().size());
            }
            return this;
        }

        /**
         * Registers all the class registered to given KryoNamespace.
         *
         * @param ns KryoNamespace
         * @return this
         */
        public Builder register(final KryoNamespace ns) {
            if (blocks.containsAll(ns.registeredBlocks)) {
                // Everything was already registered.
                log.debug("Ignoring {}, already registered.", ns);
                return this;
            }

            // Copy abstract registrations.
            for (RegistrationBlock abstractBlock : ns.registeredAbstracts) {
                registerAbstract(abstractBlock);
            }

            // Copy concrete registrations.
            for (RegistrationBlock block : ns.registeredBlocks) {
                register(block);
            }

            // Copy default serializers.
            for (Map.Entry<Class<?>, Serializer<?>> entry : ns.registeredDefaults.entrySet()) {
                registerDefault(entry.getValue(), entry.getKey());
            }
            return this;
        }

        /**
         * Sets the registrationRequired flag.
         *
         * @param registrationRequired Kryo's registrationRequired flag
         * @return this
         *
         * @see Kryo#setRegistrationRequired(boolean)
         */
        public Builder setRegistrationRequired(boolean registrationRequired) {
            this.registrationRequired = registrationRequired;
            return this;
        }
    }

    /**
     * Creates a new {@link KryoNamespace} builder.
     *
     * @return builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates a Kryo instance pool.
     *
     * @param registeredTypes types to register
     * @param registeredAbstracts abstract types to register
     * @param registeredDefaults default types to register
     * @param registrationRequired
     * @param friendlyName friendly name for the namespace
     */
    private KryoNamespace(final List<RegistrationBlock> registeredTypes,
                          final List<RegistrationBlock> registeredAbstracts,
                          final Map<Class<?>, Serializer<?>> registeredDefaults,
                          boolean registrationRequired,
                          String friendlyName) {
        this.registeredBlocks = ImmutableList.copyOf(registeredTypes);
        this.registeredAbstracts = ImmutableList.copyOf(registeredAbstracts);
        this.registeredDefaults = ImmutableMap.copyOf(registeredDefaults);
        this.registrationRequired = registrationRequired;
        this.friendlyName =  checkNotNull(friendlyName);
    }

    /**
     * Populates the Kryo pool.
     *
     * @param instances to add to the pool
     * @return this
     */
    public KryoNamespace populate(int instances) {
        for (int i = 0; i < instances; ++i) {
            release(create());
        }
        return this;
    }

    /**
     * Serializes given object to byte array using Kryo instance in pool.
     * <p>
     * Note: Serialized bytes must be smaller than {@link #MAX_BUFFER_SIZE}.
     *
     * @param obj Object to serialize
     * @return serialized bytes
     */
    public byte[] serialize(final Object obj) {
        return serialize(obj, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Serializes given object to byte array using Kryo instance in pool.
     *
     * @param obj Object to serialize
     * @param bufferSize maximum size of serialized bytes
     * @return serialized bytes
     */
    public byte[] serialize(final Object obj, final int bufferSize) {
        Output out = new Output(bufferSize, MAX_BUFFER_SIZE);
        return pool.run(kryo -> {
            kryo.writeClassAndObject(out, obj);
            out.flush();
            return out.toBytes();
        });
    }

    /**
     * Serializes given object to byte buffer using Kryo instance in pool.
     *
     * @param obj Object to serialize
     * @param buffer to write to
     */
    public void serialize(final Object obj, final ByteBuffer buffer) {
        ByteBufferOutput out = new ByteBufferOutput(buffer);
        Kryo kryo = borrow();
        try {
            kryo.writeClassAndObject(out, obj);
            out.flush();
        } finally {
            release(kryo);
        }
    }

    /**
     * Serializes given object to OutputStream using Kryo instance in pool.
     *
     * @param obj Object to serialize
     * @param stream to write to
     */
    public void serialize(final Object obj, final OutputStream stream) {
        serialize(obj, stream, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Serializes given object to OutputStream using Kryo instance in pool.
     *
     * @param obj Object to serialize
     * @param stream to write to
     * @param bufferSize size of the buffer in front of the stream
     */
    public void serialize(final Object obj, final OutputStream stream, final int bufferSize) {
        ByteBufferOutput out = new ByteBufferOutput(stream, bufferSize);
        Kryo kryo = borrow();
        try {
            kryo.writeClassAndObject(out, obj);
            out.flush();
        } finally {
            release(kryo);
        }
    }

    /**
     * Deserializes given byte array to Object using Kryo instance in pool.
     *
     * @param bytes serialized bytes
     * @param <T> deserialized Object type
     * @return deserialized Object
     */
    public <T> T deserialize(final byte[] bytes) {
        Input in = new Input(bytes);
        Kryo kryo = borrow();
        try {
            @SuppressWarnings("unchecked")
            T obj = (T) kryo.readClassAndObject(in);
            return obj;
        } finally {
            release(kryo);
        }
    }

    /**
     * Deserializes given byte buffer to Object using Kryo instance in pool.
     *
     * @param buffer input with serialized bytes
     * @param <T> deserialized Object type
     * @return deserialized Object
     */
    public <T> T deserialize(final ByteBuffer buffer) {
        ByteBufferInput in = new ByteBufferInput(buffer);
        Kryo kryo = borrow();
        try {
            @SuppressWarnings("unchecked")
            T obj = (T) kryo.readClassAndObject(in);
            return obj;
        } finally {
            release(kryo);
        }
    }

    /**
     * Deserializes given InputStream to an Object using Kryo instance in pool.
     *
     * @param stream input stream
     * @param <T> deserialized Object type
     * @return deserialized Object
     */
    public <T> T deserialize(final InputStream stream) {
        return deserialize(stream, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Deserializes given InputStream to an Object using Kryo instance in pool.
     *
     * @param stream input stream
     * @param <T> deserialized Object type
     * @return deserialized Object
     * @param bufferSize size of the buffer in front of the stream
     */
    public <T> T deserialize(final InputStream stream, final int bufferSize) {
        ByteBufferInput in = new ByteBufferInput(stream, bufferSize);
        Kryo kryo = borrow();
        try {
            @SuppressWarnings("unchecked")
            T obj = (T) kryo.readClassAndObject(in);
            return obj;
        } finally {
            release(kryo);
        }
    }

    private String friendlyName() {
        return friendlyName;
    }

    /**
     * Gets the number of classes registered in this Kryo namespace.
     *
     * @return size of namespace
     */
    public int size() {
        return (int) registeredBlocks.stream()
                .flatMap(block -> block.types().stream())
                .count();
    }

    /**
     * Creates a Kryo instance.
     *
     * @return Kryo instance
     */
    @Override
    public Kryo create() {
        log.trace("Creating Kryo instance for {}", this);
        OnosKryo kryo = new OnosKryo();
        kryo.setRegistrationRequired(registrationRequired);

        // TODO rethink whether we want to use StdInstantiatorStrategy
        kryo.setInstantiatorStrategy(
                new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        // Register default serializers.
        for (Map.Entry<Class<?>, Serializer<?>> defaultSerializer : registeredDefaults.entrySet()) {
            kryo.addDefaultSerializer(defaultSerializer.getKey(), defaultSerializer.getValue());
        }

        // Register abstract serializers.
        for (RegistrationBlock block : registeredAbstracts) {
            int id = block.begin();
            if (id == FLOATING_ID) {
                id = kryo.getNextRegistrationId();
            }
            for (Pair<Class<?>, Serializer<?>> entry : block.types()) {
                kryo.registerAbstract(entry.getLeft(), id++, entry.getRight());
            }
        }

        // Register concrete serializers.
        for (RegistrationBlock block : registeredBlocks) {
            int id = block.begin();
            if (id == FLOATING_ID) {
                id = kryo.getNextRegistrationId();
            }
            for (Pair<Class<?>, Serializer<?>> entry : block.types()) {
                register(kryo, entry.getLeft(), entry.getRight(), id++);
            }
        }
        return kryo;
    }

    /**
     * Register {@code type} and {@code serializer} to {@code kryo} instance.
     *
     * @param kryo       Kryo instance
     * @param type       type to register
     * @param serializer Specific serializer to register or null to use default.
     * @param id         type registration id to use
     */
    private void register(Kryo kryo, Class<?> type, Serializer<?> serializer, int id) {
        Registration existing = kryo.getRegistration(id);
        if (existing != null) {
            if (existing.getType() != type) {
                log.error("{}: Failed to register {} as {}, {} was already registered.",
                          friendlyName(), type, id, existing.getType());

                throw new IllegalStateException(String.format(
                          "Failed to register %s as %s, %s was already registered.",
                          type, id, existing.getType()));
            }
            // falling through to register call for now.
            // Consider skipping, if there's reasonable
            // way to compare serializer equivalence.
        }
        Registration r;
        if (serializer == null) {
            r = kryo.register(type, id);
        } else {
            r = kryo.register(type, serializer, id);
        }
        if (r.getId() != id) {
            log.warn("{}: {} already registed as {}. Skipping {}.",
                     friendlyName(), r.getType(), r.getId(), id);
        }
        log.trace("{} registered as {}", r.getType(), r.getId());
    }

    @Override
    public Kryo borrow() {
        return pool.borrow();
    }

    @Override
    public void release(Kryo kryo) {
        pool.release(kryo);
    }

    @Override
    public <T> T run(KryoCallback<T> callback) {
        return pool.run(callback);
    }

    @Override
    public String toString() {
        if (friendlyName != NO_NAME) {
            return MoreObjects.toStringHelper(getClass())
                    .omitNullValues()
                    .add("friendlyName", friendlyName)
                    // omit lengthy detail, when there's a name
                    .toString();
        }
        return MoreObjects.toStringHelper(getClass())
                    .add("registeredBlocks", registeredBlocks)
                    .toString();
    }

    static final class RegistrationBlock {
        private final int begin;
        private final ImmutableList<Pair<Class<?>, Serializer<?>>> types;

        public RegistrationBlock(int begin, List<Pair<Class<?>, Serializer<?>>> types) {
            this.begin = begin;
            this.types = ImmutableList.copyOf(types);
        }

        public int begin() {
            return begin;
        }

        public ImmutableList<Pair<Class<?>, Serializer<?>>> types() {
            return types;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                    .add("begin", begin)
                    .add("types", types)
                    .toString();
        }

        @Override
        public int hashCode() {
            return types.hashCode();
        }

        // Only the registered types are used for equality.
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj instanceof RegistrationBlock) {
                RegistrationBlock that = (RegistrationBlock) obj;
                return Objects.equals(this.types, that.types);
            }
            return false;
        }
    }

    /**
     * Special implementation of {@link Kryo} that supports abstract serializers.
     */
    private static class OnosKryo extends Kryo {
        private final OnosClassResolver classResolver;

        private OnosKryo() {
            super(new OnosClassResolver(), new MapReferenceResolver());
            this.classResolver = (OnosClassResolver) super.getClassResolver();
        }

        /**
         * Registers an abstract type.
         *
         * @param type the abstract serializable type
         * @param id the abstract type ID
         * @param serializer the abstract type serializer
         * @return this
         */
        public OnosKryo registerAbstract(Class<?> type, int id, Serializer<?> serializer) {
            classResolver.registerAbstract(new Registration(type, serializer, id));
            return this;
        }

        /**
         * Returns the abstract serializer for the given type.
         *
         * @param type the type for which to return the abstract serializer
         * @return the abstract serializer for the given type
         */
        public Serializer<?> getAbstractSerializer(Class<?> type) {
            return classResolver.getAbstractRegistration(type).getSerializer();
        }

        @Override
        public Registration register(Class type) {
            Registration registration = classResolver.getRegistration(type);
            if (registration != null) {
                return registration;
            }

            registration = classResolver.getAbstractRegistration(type);
            if (registration != null) {
                return registration;
            }

            Serializer abstractSerializer = getAbstractSerializer(type);
            if (abstractSerializer != null) {

            }
            return register(type, getDefaultSerializer(type));
        }

        @Override
        public Registration register(Class type, int id) {
            return super.register(type, id);
        }

        static final class DefaultSerializerEntry {
            final Class type;
            final SerializerFactory serializerFactory;

            DefaultSerializerEntry (Class type, SerializerFactory serializerFactory) {
                this.type = type;
                this.serializerFactory = serializerFactory;
            }
        }
    }

    /**
     * Class resolver that resolves abstract types.
     */
    private static class OnosClassResolver extends DefaultClassResolver {
        protected final ObjectMap<Class, Registration> abstractClassToRegistration = new ObjectMap();

        /**
         * Registers an abstract serializer.
         *
         * @param registration the abstract registration
         * @return the registration
         */
        public Registration registerAbstract(Registration registration) {
            checkNotNull(registration);

            if (registration.getId() == NAME) {
                throw new IllegalArgumentException("Abstract registration must contain a unique ID");
            } else {
                idToRegistration.put(registration.getId(), registration);
            }
            abstractClassToRegistration.put(registration.getType(), registration);
            return registration;
        }

        /**
         * Returns the abstract registration for the given type.
         *
         * @param type the type for which to return the abstract registration
         * @return the abstract registration for the given type
         */
        @SuppressWarnings("unchecked")
        public Registration getAbstractRegistration(Class type) {
            Registration registration = abstractClassToRegistration.get(type);
            if (registration != null) {
                return registration;
            }

            for (ObjectMap.Entry<Class, Registration> entry : abstractClassToRegistration.entries()) {
                if (entry.key.isAssignableFrom(type)) {
                    abstractClassToRegistration.put(type, entry.value);
                    return entry.value;
                }
            }
            return null;
        }

        @Override
        public Registration getRegistration(Class type) {
            Registration registration = classToRegistration.get(type);
            if (registration != null) {
                return registration;
            }

            registration = getAbstractRegistration(type);
            if (registration != null) {
                return registration;
            }
            return super.getRegistration(type);
        }
    }
}
