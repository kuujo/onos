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

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.Command;
import io.atomix.copycat.Operation;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineContext;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.session.Sessions;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.compaction.Compaction;
import io.atomix.copycat.server.storage.entry.CommandEntry;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.entry.QueryEntry;
import io.atomix.copycat.server.storage.entry.RegisterEntry;
import io.atomix.copycat.server.storage.entry.UnregisterEntry;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.session.Session;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceFactory;
import io.atomix.resource.ResourceTypeInfo;
import org.onosproject.store.primitives.impl.CatalystSerializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource state machine test.
 */
public class StateMachineTester {
    private final ResourceFactory resourceFactory;
    private final ThreadContext context;
    private final List<Entry> entries = new ArrayList<>();
    private long nextIndex;

    public StateMachineTester(Class<? extends Resource> stateMachineClass) {
        ResourceTypeInfo typeInfo = stateMachineClass.getAnnotation(ResourceTypeInfo.class);
        try {
            this.resourceFactory = typeInfo.factory().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AssertionError();
        }
        this.context = new SingleThreadContext("test-thread-%d", CatalystSerializers.getSerializer());
        context.serializer().resolve(new StorageSerialization());
    }

    private long nextIndex() {
        return ++nextIndex;
    }

    long findSessionId(long sessionId) {
        Optional<Entry> registerEntry = entries.stream()
                .filter(e -> e instanceof RegisterEntry
                        && ((RegisterEntry) e).getClient().equals(String.valueOf(sessionId)))
                .findFirst();
        if (registerEntry.isPresent()) {
            return registerEntry.get().getIndex();
        } else {
            throw new IllegalStateException("session " + sessionId + " not registered");
        }
    }

    public StateMachineTester register(long sessionId) {
        entries.add(new RegisterEntry()
                .setIndex(nextIndex())
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setClient(String.valueOf(sessionId))
                .setTimeout(5000));
        return this;
    }

    public StateMachineTester expire(long sessionId) {
        entries.add(new UnregisterEntry()
                .setIndex(nextIndex())
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setSession(findSessionId(sessionId))
                .setExpired(true));
        return this;
    }

    public StateMachineTester unregister(long sessionId) {
        entries.add(new UnregisterEntry()
                .setIndex(nextIndex())
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setSession(findSessionId(sessionId))
                .setExpired(false));
        return this;
    }

    public StateMachineTester apply(Command command) {
        entries.add(new CommandEntry()
                .setIndex(nextIndex())
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setCommand(command));
        return this;
    }

    public StateMachineTester apply(long sessionId, Command command) {
        entries.add(new CommandEntry()
                .setIndex(nextIndex())
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setSession(findSessionId(sessionId))
                .setCommand(command));
        return this;
    }

    public void test(Consumer<StateMachineAssert> checker) throws Exception {
        int iterations = 1;
        for (int segmentSize = 1; segmentSize < entries.size(); segmentSize++) {
            for (int compactionIndex = 1; compactionIndex <= entries.size(); compactionIndex++) {
                for (double compactionThreshold = .1; compactionThreshold <= 1; compactionThreshold += .1) {
                    for (Compaction compactionType : Compaction.values()) {
                        runTest(segmentSize, compactionThreshold, compactionIndex, compactionType, checker);
                        iterations++;
                    }
                }
            }
        }
        System.out.println("Ran " + iterations + " iterations");
    }

    private void runTest(int segmentSize, double compactionThreshold, long compactionIndex, Compaction compactionType, Consumer<StateMachineAssert> checker) throws Exception {
        try (Log log = initializeLog(segmentSize, compactionThreshold)) {
            TestStateMachineExecutor initialExecutor = new TestStateMachineExecutor(context.serializer());
            StateMachine initialStateMachine = resourceFactory.createStateMachine(new Properties());

            Method method = StateMachine.class.getDeclaredMethod("configure", StateMachineExecutor.class);
            method.setAccessible(true);
            method.invoke(initialStateMachine, initialExecutor);
            initialExecutor.context.sessions.listeners.add((SessionListener) initialStateMachine);

            applyEntriesUntil(initialExecutor, log, compactionIndex);
            compactLog(log, compactionType, compactionIndex);

            TestStateMachineExecutor testExecutor = new TestStateMachineExecutor(context.serializer());
            StateMachine stateMachine = resourceFactory.createStateMachine(new Properties());
            method.invoke(stateMachine, testExecutor);
            testExecutor.context.sessions.listeners.add((SessionListener) stateMachine);

            applyEntriesUntil(testExecutor, log, log.lastIndex());
            checker.accept(new StateMachineAssert(this, testExecutor, log));
        }
    }

    private Log initializeLog(int segmentSize, double compactionThreshold) {
        Storage storage = Storage.builder()
                .withStorageLevel(StorageLevel.MEMORY)
                .withMaxEntriesPerSegment(segmentSize)
                .withCompactionThreads(1)
                .withMajorCompactionInterval(Duration.ofMillis(Long.MAX_VALUE))
                .withMinorCompactionInterval(Duration.ofMillis(Long.MAX_VALUE))
                .withCompactionThreshold(compactionThreshold)
                .build();

        Log log = context.execute(() -> storage.openLog("test")).join();
        for (Entry entry : entries) {
            log.append(entry);
        }
        return log;
    }

    private void compactLog(Log log, Compaction compaction, long index) {
        log.compactor().minorIndex(index);
        log.compactor().majorIndex(index);
        log.compactor().snapshotIndex(index);
        log.compactor().compact(compaction).join();
    }

    @SuppressWarnings("unchecked")
    private void applyEntriesUntil(TestStateMachineExecutor testExecutor, Log log, long lastIndex) {
        for (long i = 1; i <= lastIndex; i++) {
            applyEntry(testExecutor, log, i);
        }
    }

    private void applyEntry(TestStateMachineExecutor testExecutor, Log log, long index) {
        try (Entry entry = log.get(index)) {
            applyEntry(testExecutor, log, entry);
        }
    }

    @SuppressWarnings("unchecked")
    <T> T applyEntry(TestStateMachineExecutor testExecutor, Log log, Entry entry) {
        if (entry == null) {
            return null;
        } else if (entry instanceof RegisterEntry) {
            TestSession session = new TestSession(entry.getIndex(), testExecutor.context.sessions);
            testExecutor.context.index = entry.getIndex();
            testExecutor.context.clock = Clock.fixed(Instant.ofEpochMilli(((RegisterEntry) entry).getTimestamp()), ZoneId.systemDefault());
            testExecutor.context.sessions.sessions.put(session.id, session);
            testExecutor.context.sessions.listeners.forEach(l -> l.register(session));
        } else if (entry instanceof UnregisterEntry) {
            UnregisterEntry unregister = (UnregisterEntry) entry;
            testExecutor.context.index = entry.getIndex();
            testExecutor.context.clock = Clock.fixed(Instant.ofEpochMilli(unregister.getTimestamp()), ZoneId.systemDefault());
            TestSession session = testExecutor.context.sessions.sessions.remove(unregister.getSession());
            if (unregister.isExpired()) {
                testExecutor.context.sessions.listeners.forEach(l -> l.expire(session));
            } else {
                testExecutor.context.sessions.listeners.forEach(l -> l.unregister(session));
            }
            testExecutor.context.sessions.listeners.forEach(l -> l.close(session));
        } else if (entry instanceof CommandEntry) {
            CommandEntry command = (CommandEntry) entry;
            testExecutor.context.index = entry.getIndex();
            testExecutor.context.clock = Clock.fixed(Instant.ofEpochMilli(((CommandEntry) entry).getTimestamp()), ZoneId.systemDefault());
            TestSession session = testExecutor.context.sessions.sessions.get(command.getSession());
            TestCommit commit = new TestCommit<>(command.getIndex(), session, Instant.ofEpochMilli(command.getTimestamp()), command.getCommand(), log);
            return (T) testExecutor.execute(commit);
        } else if (entry instanceof QueryEntry) {
            QueryEntry query = (QueryEntry) entry;
            TestSession session = testExecutor.context.sessions.sessions.get(query.getSession());
            TestCommit commit = new TestCommit<>(query.getIndex(), session, Instant.ofEpochMilli(query.getTimestamp()), query.getQuery(), log);
            return (T) testExecutor.execute(commit);
        } else {
            throw new UnsupportedOperationException();
        }
        return null;
    }

    static class TestCommit<T extends Operation> implements Commit<T> {
        private final long index;
        private final ServerSession session;
        private final Instant time;
        private final T operation;
        private final Log log;
        private final AtomicInteger references = new AtomicInteger(1);

        TestCommit(long index, ServerSession session, Instant time, T operation, Log log) {
            this.index = index;
            this.session = session;
            this.time = time;
            this.operation = operation;
            this.log = log;
        }

        @Override
        public long index() {
            return index;
        }

        @Override
        public ServerSession session() {
            return session;
        }

        @Override
        public Instant time() {
            return time;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<T> type() {
            return (Class<T>) operation.getClass();
        }

        @Override
        public T operation() {
            return operation;
        }

        @Override
        public Commit<T> acquire() {
            references.incrementAndGet();
            return this;
        }

        @Override
        public boolean release() {
            if (references.decrementAndGet() == 0) {
                close();
                return true;
            }
            return false;
        }

        @Override
        public int references() {
            return references.get();
        }

        @Override
        public void close() {
            if (operation instanceof Command && index <= log.lastIndex()) {
                log.release(index);
            }
        }
    }

    static class TestStateMachineExecutor implements StateMachineExecutor {
        private static final Logger LOGGER = LoggerFactory.getLogger(TestStateMachineExecutor.class);

        private final Serializer serializer;
        final TestStateMachineContext context = new TestStateMachineContext();
        final Map<Class, Function<Commit, Object>> callbacks = new HashMap<>();

        private TestStateMachineExecutor(Serializer serializer) {
            this.serializer = serializer;
        }

        @Override
        public StateMachineContext context() {
            return context;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Operation<Void>> StateMachineExecutor register(Class<T> type, Consumer<Commit<T>> callback) {
            callbacks.put(type, a -> {
                callback.accept(a);
                return null;
            });
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Operation<U>, U> StateMachineExecutor register(Class<T> type, Function<Commit<T>, U> callback) {
            callbacks.put(type, (Function) callback);
            return this;
        }

        @SuppressWarnings("unchecked")
        <T extends Operation<U>, U> U execute(Commit<T> commit) {
            Function<Commit, Object> callback = callbacks.get(commit.type());
            if (callback == null) {
                throw new UnsupportedOperationException();
            }
            return (U) callback.apply(commit);
        }

        @Override
        public Logger logger() {
            return LOGGER;
        }

        @Override
        public Serializer serializer() {
            return serializer;
        }

        @Override
        public Executor executor() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Scheduled schedule(Duration delay, Runnable callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
            throw new UnsupportedOperationException();
        }
    }

    static class TestStateMachineContext implements StateMachineContext {
        long index;
        Clock clock;
        TestSessions sessions = new TestSessions(this);

        @Override
        public long index() {
            return index;
        }

        @Override
        public Clock clock() {
            return clock;
        }

        @Override
        public Sessions sessions() {
            return sessions;
        }
    }

    static class TestSessions implements Sessions {
        final TestStateMachineContext context;
        final Map<Long, TestSession> sessions = new HashMap<>();
        final Set<SessionListener> listeners = new HashSet<>();

        TestSessions(TestStateMachineContext context) {
            this.context = context;
        }

        @Override
        public ServerSession session(long sessionId) {
            return sessions.get(sessionId);
        }

        @Override
        public Sessions addListener(SessionListener listener) {
            listeners.add(listener);
            return this;
        }

        @Override
        public Sessions removeListener(SessionListener listener) {
            listeners.remove(listener);
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Iterator<ServerSession> iterator() {
            return (Iterator) sessions.values().iterator();
        }
    }

    static class TestSession implements ServerSession {
        final TestSessions sessions;
        final long id;
        State state = State.OPEN;
        final Map<Long, Map<String, List<Object>>> publishedEvents = new HashMap<>();

        TestSession(long id, TestSessions sessions) {
            this.id = id;
            this.sessions = sessions;
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public State state() {
            return state;
        }

        @Override
        public Session publish(String event) {
            return publish(event, null);
        }

        @Override
        public Session publish(String event, Object message) {
            Map<String, List<Object>> indexEvents = publishedEvents.computeIfAbsent(sessions.context.index, i -> new HashMap<>());
            List<Object> topicEvents = indexEvents.computeIfAbsent(event, e -> new ArrayList<>());
            topicEvents.add(message);
            return this;
        }

        @Override
        public Listener<State> onStateChange(Consumer<State> callback) {
            return null;
        }
    }

}
