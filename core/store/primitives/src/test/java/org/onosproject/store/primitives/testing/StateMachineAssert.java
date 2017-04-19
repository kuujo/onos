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

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.entry.CommandEntry;
import io.atomix.copycat.server.storage.entry.QueryEntry;

/**
 * State machine assertion.
 */
public class StateMachineAssert {
    private final StateMachineTester tester;
    private final StateMachineTester.TestStateMachineExecutor testExecutor;
    private final Log log;
    private long nextIndex;

    public StateMachineAssert(StateMachineTester tester, StateMachineTester.TestStateMachineExecutor testExecutor, Log log) {
        this.tester = tester;
        this.testExecutor = testExecutor;
        this.log = log;
        this.nextIndex = log.nextIndex();
    }

    private long nextIndex() {
        return nextIndex++;
    }

    public <T> QueryAssert<T> query(Query<T> query) {
        QueryEntry entry = new QueryEntry()
                .setIndex(nextIndex())
                .setTimestamp(System.currentTimeMillis())
                .setQuery(query);
        return new QueryAssert<>(tester.applyEntry(testExecutor, log, entry));
    }

    public <T> QueryAssert<T> query(long sessionId, Query<T> query) {
        QueryEntry entry = new QueryEntry()
                .setIndex(nextIndex())
                .setTimestamp(System.currentTimeMillis())
                .setSession(tester.findSessionId(sessionId))
                .setQuery(query);
        return new QueryAssert<>(tester.applyEntry(testExecutor, log, entry));
    }

    public <T> CommandAssert<T> command(Command<T> command) {
        CommandEntry entry = new CommandEntry()
                .setIndex(nextIndex())
                .setTimestamp(System.currentTimeMillis())
                .setCommand(command);
        return new CommandAssert<>(tester.applyEntry(testExecutor, log, entry), testExecutor);
    }

    public <T> CommandAssert<T> command(long sessionId, Command<T> command) {
        CommandEntry entry = new CommandEntry()
                .setIndex(nextIndex())
                .setTimestamp(System.currentTimeMillis())
                .setSession(tester.findSessionId(sessionId))
                .setCommand(command);
        return new CommandAssert<>(tester.applyEntry(testExecutor, log, entry), testExecutor);
    }

}
