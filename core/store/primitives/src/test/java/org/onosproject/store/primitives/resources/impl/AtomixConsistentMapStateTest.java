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

import java.util.Arrays;
import java.util.Objects;

import org.junit.Test;
import org.onlab.util.Match;
import org.onosproject.store.primitives.testing.StateMachineTester;
import org.onosproject.store.service.Versioned;

import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.Get;
import static org.onosproject.store.primitives.resources.impl.AtomixConsistentMapCommands.UpdateAndGet;

/**
 * Atomic consistent map state machine test.
 */
public class AtomixConsistentMapStateTest {

    @Test
    public void testConsistentMapState() throws Throwable {
        StateMachineTester tester = new StateMachineTester(AtomixConsistentMap.class);
        tester.register(1);
        tester.register(2);
        tester.apply(new UpdateAndGet("1", "1".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("2", "2".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("3", "3".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("4", "4".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("5", "5".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("6", "6".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("7", "7".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("8", "8".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("9", "9".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("10", "10".getBytes(), Match.ANY, Match.ANY));

        tester.apply(new UpdateAndGet("1", "2".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("2", "3".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("3", "4".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("4", "5".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("5", "6".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("6", "7".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("7", "8".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("8", "9".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("9", "10".getBytes(), Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("10", "11".getBytes(), Match.ANY, Match.ANY));

        tester.apply(new UpdateAndGet("6", null, Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("7", null, Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("8", null, Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("9", null, Match.ANY, Match.ANY));
        tester.apply(new UpdateAndGet("10", null, Match.ANY, Match.ANY));

        tester.test(stateMachine -> {
            stateMachine.query(new Get("1")).returnMatches(v -> Arrays.equals(v.value(), "2".getBytes()) && v.version() == 13);
            stateMachine.query(new Get("2")).returnMatches(v -> Arrays.equals(v.value(), "3".getBytes()) && v.version() == 14);
            stateMachine.query(new Get("3")).returnMatches(v -> Arrays.equals(v.value(), "4".getBytes()) && v.version() == 15);
            stateMachine.query(new Get("4")).returnMatches(v -> Arrays.equals(v.value(), "5".getBytes()) && v.version() == 16);
            stateMachine.query(new Get("5")).returnMatches(v -> Arrays.equals(v.value(), "6".getBytes()) && v.version() == 17);

            stateMachine.query(new Get("6")).returnsNull();
            stateMachine.query(new Get("7")).returnsNull();
            stateMachine.query(new Get("8")).returnsNull();
            stateMachine.query(new Get("9")).returnsNull();
            stateMachine.query(new Get("10")).returnsNull();
        });
    }

}
