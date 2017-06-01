/*
 * Copyright 2016-present Open Networking Laboratory
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import org.onlab.util.KryoNamespace;
import org.onlab.util.Match;
import org.onosproject.store.primitives.MapUpdate;
import org.onosproject.store.primitives.TransactionId;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.Clear;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.ContainsKey;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.ContainsValue;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.EntrySet;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.Get;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.GetOrDefault;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.IsEmpty;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.KeySet;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.Listen;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.Size;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.TransactionBegin;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.TransactionCommit;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.TransactionPrepare;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.TransactionPrepareAndCommit;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.TransactionRollback;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.Unlisten;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.UpdateAndGet;
import org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.Values;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.MapEvent;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.TransactionLog;
import org.onosproject.store.service.Versioned;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkState;
import static org.onosproject.store.service.MapEvent.Type.INSERT;
import static org.onosproject.store.service.MapEvent.Type.REMOVE;
import static org.onosproject.store.service.MapEvent.Type.UPDATE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * State Machine for {@link AtomixConsistentMap} resource.
 */
public class AtomixConsistentMapState extends StateMachine implements SessionListener, Snapshottable {

    private final Logger log = getLogger(getClass());
    private final Serializer serializer = Serializer.using(KryoNamespace.newBuilder()
            .register(KryoNamespaces.BASIC)
            .register(TransactionId.class)
            .register(TransactionScope.class)
            .register(MapEntryValue.class)
            .build());
    private final Map<Long, ServerSession> listeners = new HashMap<>();
    private final Map<String, MapEntryValue> mapEntries = new HashMap<>();
    private final Set<String> preparedKeys = Sets.newHashSet();
    private final Map<TransactionId, TransactionScope> activeTransactions = Maps.newHashMap();
    private long currentVersion;

    @Override
    public void snapshot(SnapshotWriter writer) {
        byte[] listenersBytes = serializer.encode(listeners.keySet());
        writer.writeInt(listenersBytes.length);
        writer.write(listenersBytes);

        byte[] preparedKeysBytes = serializer.encode(preparedKeys);
        writer.writeInt(preparedKeysBytes.length);
        writer.write(preparedKeysBytes);

        byte[] mapEntriesBytes = serializer.encode(mapEntries);
        writer.writeInt(mapEntriesBytes.length);
        writer.write(mapEntriesBytes);

        byte[] activeTransactionsBytes = serializer.encode(activeTransactions);
        writer.writeInt(activeTransactionsBytes.length);
        writer.write(activeTransactionsBytes);

        writer.writeLong(currentVersion);
    }

    @Override
    public void install(SnapshotReader reader) {
        listeners.clear();
        int listenersLength = reader.readInt();
        byte[] listenersBytes = reader.readBytes(listenersLength);
        for (long sessionId : serializer.<Set<Long>>decode(listenersBytes)) {
            listeners.put(sessionId, sessions.session(sessionId));
        }

        preparedKeys.clear();
        int preparedKeysLength = reader.readInt();
        byte[] preparedKeysBytes = reader.readBytes(preparedKeysLength);
        preparedKeys.addAll(serializer.decode(preparedKeysBytes));

        mapEntries.clear();
        int mapEntriesLength = reader.readInt();
        byte[] mapEntriesBytes = reader.readBytes(mapEntriesLength);
        mapEntries.putAll(serializer.decode(mapEntriesBytes));

        activeTransactions.clear();
        int activeTransactionsLength = reader.readInt();
        byte[] activeTransactionsBytes = reader.readBytes(activeTransactionsLength);
        activeTransactions.putAll(serializer.decode(activeTransactionsBytes));

        currentVersion = reader.readLong();
    }

    @Override
    protected void configure(StateMachineExecutor executor) {
        // Listeners
        executor.register(Listen.class, this::listen);
        executor.register(Unlisten.class, this::unlisten);
        // Queries
        executor.register(ContainsKey.class, this::containsKey);
        executor.register(ContainsValue.class, this::containsValue);
        executor.register(EntrySet.class, this::entrySet);
        executor.register(Get.class, this::get);
        executor.register(GetOrDefault.class, this::getOrDefault);
        executor.register(IsEmpty.class, this::isEmpty);
        executor.register(KeySet.class, this::keySet);
        executor.register(Size.class, this::size);
        executor.register(Values.class, this::values);
        // Commands
        executor.register(UpdateAndGet.class, this::updateAndGet);
        executor.register(AtomixConsistentMapCommands.Clear.class, this::clear);
        executor.register(TransactionBegin.class, this::begin);
        executor.register(TransactionPrepare.class, this::prepare);
        executor.register(TransactionCommit.class, this::commit);
        executor.register(TransactionRollback.class, this::rollback);
        executor.register(TransactionPrepareAndCommit.class, this::prepareAndCommit);
    }

    /**
     * Handles a contains key commit.
     *
     * @param commit containsKey commit
     * @return {@code true} if map contains key
     */
    protected boolean containsKey(Commit<? extends ContainsKey> commit) {
        MapEntryValue value = mapEntries.get(commit.operation().key());
        return value != null && value.type() != MapEntryValue.Type.TOMBSTONE;
    }

    /**
     * Handles a contains value commit.
     *
     * @param commit containsValue commit
     * @return {@code true} if map contains value
     */
    protected boolean containsValue(Commit<? extends ContainsValue> commit) {
        Match<byte[]> valueMatch = Match.ifValue(commit.operation().value());
        return mapEntries.values().stream()
                .filter(value -> value.type() != MapEntryValue.Type.TOMBSTONE)
                .anyMatch(value -> valueMatch.matches(value.value()));
    }

    /**
     * Handles a get commit.
     *
     * @param commit get commit
     * @return value mapped to key
     */
    protected Versioned<byte[]> get(Commit<? extends Get> commit) {
        return toVersioned(mapEntries.get(commit.operation().key()));
    }

    /**
     * Handles a get or default commit.
     *
     * @param commit get or default commit
     * @return value mapped to key
     */
    protected Versioned<byte[]> getOrDefault(Commit<? extends GetOrDefault> commit) {
        MapEntryValue value = mapEntries.get(commit.operation().key());
        if (value == null) {
            return new Versioned<>(commit.operation().defaultValue(), 0);
        } else if (value.type() == MapEntryValue.Type.TOMBSTONE) {
            return new Versioned<>(commit.operation().defaultValue(), value.version);
        } else {
            return new Versioned<>(value.value(), value.version);
        }
    }

    /**
     * Handles a count commit.
     *
     * @param commit size commit
     * @return number of entries in map
     */
    protected int size(Commit<? extends Size> commit) {
        return (int) mapEntries.values().stream()
                .filter(value -> value.type() != MapEntryValue.Type.TOMBSTONE)
                .count();
    }

    /**
     * Handles an is empty commit.
     *
     * @param commit isEmpty commit
     * @return {@code true} if map is empty
     */
    protected boolean isEmpty(Commit<? extends IsEmpty> commit) {
        return mapEntries.values().stream()
                .noneMatch(value -> value.type() != MapEntryValue.Type.TOMBSTONE);
    }

    /**
     * Handles a keySet commit.
     *
     * @param commit keySet commit
     * @return set of keys in map
     */
    protected Set<String> keySet(Commit<? extends KeySet> commit) {
        return mapEntries.entrySet().stream()
                .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Handles a values commit.
     *
     * @param commit values commit
     * @return collection of values in map
     */
    protected Collection<Versioned<byte[]>> values(Commit<? extends Values> commit) {
        return mapEntries.entrySet().stream()
                .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE)
                .map(entry -> toVersioned(entry.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Handles a entry set commit.
     *
     * @param commit entrySet commit
     * @return set of map entries
     */
    protected Set<Map.Entry<String, Versioned<byte[]>>> entrySet(Commit<? extends EntrySet> commit) {
        return mapEntries.entrySet().stream()
                .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE)
                .map(e -> Maps.immutableEntry(e.getKey(), toVersioned(e.getValue())))
                .collect(Collectors.toSet());
    }

    /**
     * Handles a update and get commit.
     *
     * @param commit updateAndGet commit
     * @return update result
     */
    protected MapEntryUpdateResult<String, byte[]> updateAndGet(Commit<? extends UpdateAndGet> commit) {
        try {
            MapEntryUpdateResult.Status updateStatus = validate(commit.operation());
            String key = commit.operation().key();
            MapEntryValue oldCommitValue = mapEntries.get(commit.operation().key());
            Versioned<byte[]> oldMapValue = toVersioned(oldCommitValue);

            if (updateStatus != MapEntryUpdateResult.Status.OK) {
                return new MapEntryUpdateResult<>(updateStatus, "", key, oldMapValue, oldMapValue);
            }

            byte[] newValue = commit.operation().value();
            currentVersion = commit.index();
            Versioned<byte[]> newMapValue = newValue == null ? null
                    : new Versioned<>(newValue, currentVersion);

            MapEvent.Type updateType = newValue == null ? REMOVE
                    : oldCommitValue == null ? INSERT : UPDATE;

            // If a value existed in the map, remove and discard the value to ensure disk can be freed.
            if (updateType == REMOVE || updateType == UPDATE) {
                mapEntries.remove(key);
            }

            // If this is an insert/update commit, add the commit to the map entries.
            if (updateType == INSERT || updateType == UPDATE) {
                mapEntries.put(key, new MapEntryValue(MapEntryValue.Type.VALUE, commit.index(), commit.operation().value()));
            } else if (!activeTransactions.isEmpty()) {
                // If this is a delete but transactions are currently running, ensure tombstones are retained
                // for version checks.
                mapEntries.put(key, new MapEntryValue(MapEntryValue.Type.TOMBSTONE, commit.index(), null));
            }

            publish(Lists.newArrayList(new MapEvent<>("", key, newMapValue, oldMapValue)));
            return new MapEntryUpdateResult<>(updateStatus, "", key, oldMapValue, newMapValue);
        } catch (Exception e) {
            log.error("State machine operation failed", e);
            throw Throwables.propagate(e);
        }
    }

    /**
     * Handles a clear commit.
     *
     * @param commit clear commit
     * @return clear result
     */
    protected MapEntryUpdateResult.Status clear(Commit<? extends Clear> commit) {
        Iterator<Map.Entry<String, MapEntryValue>> iterator = mapEntries
                .entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, MapEntryValue> entry = iterator.next();
            String key = entry.getKey();
            MapEntryValue value = entry.getValue();
            Versioned<byte[]> removedValue = new Versioned<>(value.value(),
                    value.version());
            publish(Lists.newArrayList(new MapEvent<>("", key, null, removedValue)));
            iterator.remove();
        }
        return MapEntryUpdateResult.Status.OK;
    }

    /**
     * Handles a listen commit.
     *
     * @param commit listen commit
     */
    protected void listen(Commit<? extends Listen> commit) {
        listeners.put(commit.session().id(), commit.session());
    }

    /**
     * Handles an unlisten commit.
     *
     * @param commit unlisten commit
     */
    protected void unlisten(Commit<? extends Unlisten> commit) {
        listeners.remove(commit.session().id());
    }

    /**
     * Handles a begin commit.
     *
     * @param commit transaction begin commit
     * @return transaction state version
     */
    protected long begin(Commit<? extends TransactionBegin> commit) {
        long version = commit.index();
        activeTransactions.put(commit.operation().transactionId(), new TransactionScope(version));
        return version;
    }

    /**
     * Handles an prepare and commit commit.
     *
     * @param commit transaction prepare and commit commit
     * @return prepare result
     */
    protected PrepareResult prepareAndCommit(Commit<? extends TransactionPrepareAndCommit> commit) {
        TransactionId transactionId = commit.operation().transactionLog().transactionId();
        PrepareResult prepareResult = prepare(commit);
        TransactionScope transactionScope = activeTransactions.remove(transactionId);
        if (prepareResult == PrepareResult.OK) {
            this.currentVersion = commit.index();
            transactionScope = transactionScope.prepared(commit);
            commit(transactionScope);
        }
        discardTombstones();
        return prepareResult;
    }

    /**
     * Handles an prepare commit.
     *
     * @param commit transaction prepare commit
     * @return prepare result
     */
    protected PrepareResult prepare(Commit<? extends TransactionPrepare> commit) {
        boolean ok = false;

        try {
            TransactionLog<MapUpdate<String, byte[]>> transactionLog = commit.operation().transactionLog();

            // Iterate through records in the transaction log and perform isolation checks.
            for (MapUpdate<String, byte[]> record : transactionLog.records()) {
                String key = record.key();

                // If the record is a VERSION_MATCH then check that the record's version matches the current
                // version of the state machine.
                if (record.type() == MapUpdate.Type.VERSION_MATCH && key == null) {
                    if (record.version() > currentVersion) {
                        return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
                    } else {
                        continue;
                    }
                }

                // If the prepared keys already contains the key contained within the record, that indicates a
                // conflict with a concurrent transaction.
                if (preparedKeys.contains(key)) {
                    return PrepareResult.CONCURRENT_TRANSACTION;
                }

                // Read the existing value from the map.
                MapEntryValue existingValue = mapEntries.get(key);

                // Note: if the existing value is null, that means the key has not changed during the transaction,
                // otherwise a tombstone would have been retained.
                if (existingValue == null) {
                    // If the value is null, ensure the version is equal to the transaction version.
                    if (record.version() != transactionLog.version()) {
                        return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
                    }
                } else {
                    // If the value is non-null, compare the current version with the record version.
                    if (existingValue.version() > record.version()) {
                        return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
                    }
                }
            }

            // No violations detected. Mark modified keys locked for transactions.
            transactionLog.records().forEach(record -> {
                if (record.type() != MapUpdate.Type.VERSION_MATCH) {
                    preparedKeys.add(record.key());
                }
            });

            ok = true;

            // Update the transaction scope. If the transaction scope is not set on this node, that indicates the
            // coordinator is communicating with another node. Transactions assume that the client is communicating
            // with a single leader in order to limit the overhead of retaining tombstones.
            TransactionScope transactionScope = activeTransactions.get(transactionLog.transactionId());
            if (transactionScope == null) {
                activeTransactions.put(
                        transactionLog.transactionId(),
                        new TransactionScope(transactionLog.version(), commit));
                return PrepareResult.PARTIAL_FAILURE;
            } else {
                activeTransactions.put(
                        transactionLog.transactionId(),
                        transactionScope.prepared(commit));
                return PrepareResult.OK;
            }
        } catch (Exception e) {
            log.warn("Failure applying {}", commit, e);
            throw Throwables.propagate(e);
        }
    }

    /**
     * Handles an commit commit (ha!).
     *
     * @param commit transaction commit commit
     * @return commit result
     */
    protected CommitResult commit(Commit<? extends TransactionCommit> commit) {
        TransactionId transactionId = commit.operation().transactionId();
        TransactionScope transactionScope = activeTransactions.remove(transactionId);
        if (transactionScope == null) {
            return CommitResult.UNKNOWN_TRANSACTION_ID;
        }

        try {
            this.currentVersion = commit.index();
            return commit(transactionScope.committed(commit));
        } catch (Exception e) {
            log.warn("Failure applying {}", commit, e);
            throw Throwables.propagate(e);
        } finally {
            discardTombstones();
        }
    }

    /**
     * Applies committed operations to the state machine.
     */
    private CommitResult commit(TransactionScope transactionScope) {
        TransactionLog<MapUpdate<String, byte[]>> transactionLog = transactionScope.transactionLog();
        boolean retainTombstones = !activeTransactions.isEmpty();

        // Count the total number of keys that will be set by this transaction. This is necessary to do reference
        // counting for garbage collection.
        long totalReferencesToCommit = transactionLog.records().stream()
                // No keys are set for version checks. For deletes, references are only retained of tombstones
                // need to be retained for concurrent transactions.
                .filter(record -> record.type() != MapUpdate.Type.VERSION_MATCH && record.type() != MapUpdate.Type.LOCK
                        && (record.type() != MapUpdate.Type.REMOVE_IF_VERSION_MATCH || retainTombstones))
                .count();

        List<MapEvent<String, byte[]>> eventsToPublish = Lists.newArrayList();
        for (MapUpdate<String, byte[]> record : transactionLog.records()) {
            if (record.type() == MapUpdate.Type.VERSION_MATCH) {
                continue;
            }

            String key = record.key();
            checkState(preparedKeys.remove(key), "key is not prepared");

            if (record.type() == MapUpdate.Type.LOCK) {
                continue;
            }

            MapEntryValue previousValue = mapEntries.remove(key);
            MapEntryValue newValue = null;

            // If the record is not a delete, create a transactional commit.
            if (record.type() != MapUpdate.Type.REMOVE_IF_VERSION_MATCH) {
                newValue = new MapEntryValue(MapEntryValue.Type.VALUE, currentVersion, record.value());
            } else if (retainTombstones) {
                // For deletes, if tombstones need to be retained then create and store a tombstone commit.
                newValue = new MapEntryValue(MapEntryValue.Type.TOMBSTONE, currentVersion, null);
            }

            eventsToPublish.add(new MapEvent<>("", key, toVersioned(newValue), toVersioned(previousValue)));

            if (newValue != null) {
                mapEntries.put(key, newValue);
            }
        }
        publish(eventsToPublish);
        return CommitResult.OK;
    }

    /**
     * Handles an rollback commit (ha!).
     *
     * @param commit transaction rollback commit
     * @return rollback result
     */
    protected RollbackResult rollback(Commit<? extends TransactionRollback> commit) {
        TransactionId transactionId = commit.operation().transactionId();
        TransactionScope transactionScope = activeTransactions.remove(transactionId);
        if (transactionScope == null) {
            return RollbackResult.UNKNOWN_TRANSACTION_ID;
        } else if (!transactionScope.isPrepared()) {
            discardTombstones();
            return RollbackResult.OK;
        } else {
            try {
                transactionScope.transactionLog().records()
                        .forEach(record -> {
                            if (record.type() != MapUpdate.Type.VERSION_MATCH) {
                                preparedKeys.remove(record.key());
                            }
                        });
                return RollbackResult.OK;
            } finally {
                discardTombstones();
            }
        }

    }

    /**
     * Discards tombstones no longer needed by active transactions.
     */
    private void discardTombstones() {
        if (activeTransactions.isEmpty()) {
            Iterator<Map.Entry<String, MapEntryValue>> iterator = mapEntries.entrySet().iterator();
            while (iterator.hasNext()) {
                MapEntryValue value = iterator.next().getValue();
                if (value.type() == MapEntryValue.Type.TOMBSTONE) {
                    iterator.remove();
                }
            }
        } else {
            long lowWaterMark = activeTransactions.values().stream()
                    .mapToLong(TransactionScope::version)
                    .min().getAsLong();
            Iterator<Map.Entry<String, MapEntryValue>> iterator = mapEntries.entrySet().iterator();
            while (iterator.hasNext()) {
                MapEntryValue value = iterator.next().getValue();
                if (value.type() == MapEntryValue.Type.TOMBSTONE && value.version < lowWaterMark) {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * Computes the update status that would result if the specified update were to applied to
     * the state machine.
     *
     * @param update update
     * @return status
     */
    private MapEntryUpdateResult.Status validate(UpdateAndGet update) {
        MapEntryValue existingValue = mapEntries.get(update.key());
        boolean isEmpty = existingValue == null || existingValue.type() == MapEntryValue.Type.TOMBSTONE;
        if (isEmpty && update.value() == null) {
            return MapEntryUpdateResult.Status.NOOP;
        }
        if (preparedKeys.contains(update.key())) {
            return MapEntryUpdateResult.Status.WRITE_LOCK;
        }
        byte[] existingRawValue = isEmpty ? null : existingValue.value();
        Long existingVersion = isEmpty ? null : existingValue.version();
        return update.valueMatch().matches(existingRawValue)
                && update.versionMatch().matches(existingVersion) ? MapEntryUpdateResult.Status.OK
                : MapEntryUpdateResult.Status.PRECONDITION_FAILED;
    }

    /**
     * Utility for turning a {@code MapEntryValue} to {@code Versioned}.
     * @param value map entry value
     * @return versioned instance
     */
    private Versioned<byte[]> toVersioned(MapEntryValue value) {
        return value != null && value.type() != MapEntryValue.Type.TOMBSTONE
                ? new Versioned<>(value.value(), value.version()) : null;
    }

    /**
     * Publishes events to listeners.
     *
     * @param events list of map event to publish
     */
    private void publish(List<MapEvent<String, byte[]>> events) {
        listeners.values().forEach(session -> session.publish(AtomixConsistentMap.CHANGE_SUBJECT, events));
    }

    @Override
    public void register(ServerSession session) {
    }

    @Override
    public void unregister(ServerSession session) {
        closeListener(session.id());
    }

    @Override
    public void expire(ServerSession session) {
        closeListener(session.id());
    }

    @Override
    public void close(ServerSession session) {
        closeListener(session.id());
    }

    private void closeListener(Long sessionId) {
        listeners.remove(sessionId);
    }

    /**
     * Interface implemented by map values.
     */
    private static class MapEntryValue {
        protected final Type type;
        protected final long version;
        protected final byte[] value;

        MapEntryValue(Type type, long version, byte[] value) {
            this.type = type;
            this.version = version;
            this.value = value;
        }

        /**
         * Returns the value type.
         *
         * @return the value type
         */
        Type type() {
            return type;
        }

        /**
         * Returns the version of the value.
         *
         * @return version
         */
        long version() {
            return version;
        }

        /**
         * Returns the raw {@code byte[]}.
         *
         * @return raw value
         */
        byte[] value() {
            return value;
        }

        /**
         * Value type.
         */
        enum Type {
            VALUE,
            TOMBSTONE,
        }
    }

    /**
     * Map transaction scope.
     */
    private static final class TransactionScope {
        private final long version;
        private final Commit<? extends TransactionPrepare> prepareCommit;
        private final Commit<? extends TransactionCommit> commitCommit;

        private TransactionScope(long version) {
            this(version, null, null);
        }

        private TransactionScope(
                long version,
                Commit<? extends TransactionPrepare> prepareCommit) {
            this(version, prepareCommit, null);
        }

        private TransactionScope(
                long version,
                Commit<? extends TransactionPrepare> prepareCommit,
                Commit<? extends TransactionCommit> commitCommit) {
            this.version = version;
            this.prepareCommit = prepareCommit;
            this.commitCommit = commitCommit;
        }

        /**
         * Returns the transaction version.
         *
         * @return the transaction version
         */
        long version() {
            return version;
        }

        /**
         * Returns whether this is a prepared transaction scope.
         *
         * @return whether this is a prepared transaction scope
         */
        boolean isPrepared() {
            return prepareCommit != null;
        }

        /**
         * Returns the transaction commit log.
         *
         * @return the transaction commit log
         */
        TransactionLog<MapUpdate<String, byte[]>> transactionLog() {
            checkState(isPrepared());
            return prepareCommit.operation().transactionLog();
        }

        /**
         * Returns a new transaction scope with a prepare commit.
         *
         * @param commit the prepare commit
         * @return new transaction scope updated with the prepare commit
         */
        TransactionScope prepared(Commit<? extends TransactionPrepare> commit) {
            return new TransactionScope(version, commit);
        }

        /**
         * Returns a new transaction scope with a commit commit.
         *
         * @param commit the commit commit ;-)
         * @return new transaction scope updated with the commit commit
         */
        TransactionScope committed(Commit<? extends TransactionCommit> commit) {
            checkState(isPrepared());
            return new TransactionScope(version, prepareCommit, commit);
        }
    }
}
