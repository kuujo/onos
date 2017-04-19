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

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Command assertion.
 */
public class CommandAssert<T> extends OperationAssert<CommandAssert<T>, T> {
    private final StateMachineTester.TestStateMachineExecutor testExecutor;

    CommandAssert(T returnValue, StateMachineTester.TestStateMachineExecutor testExecutor) {
        super(returnValue);
        this.testExecutor = testExecutor;
    }

    public CommandAssert<T> publishes(long sessionId, String topic, Object event) {
        StateMachineTester.TestSession session = testExecutor.context.sessions.sessions.get(sessionId);
        assertNotNull(session);

        Map<String, List<Object>> indexEvents = session.publishedEvents.get(testExecutor.context.index);
        assertNotNull(indexEvents);

        List<Object> topicEvents = indexEvents.get(topic);
        assertNotNull(topicEvents);

        boolean matched = false;
        for (Object value : topicEvents) {
            if (value != null && event != null && value.equals(event)) {
                matched = true;
                break;
            }
        }
        assertTrue(matched);
        return this;
    }
}
