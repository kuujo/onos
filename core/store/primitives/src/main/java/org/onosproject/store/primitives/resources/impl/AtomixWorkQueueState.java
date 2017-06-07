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

import static org.slf4j.LoggerFactory.getLogger;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.onlab.util.KryoNamespace;
import org.onosproject.store.primitives.resources.impl.AtomixWorkQueueCommands.Add;
import org.onosproject.store.primitives.resources.impl.AtomixWorkQueueCommands.Clear;
import org.onosproject.store.primitives.resources.impl.AtomixWorkQueueCommands.Complete;
import org.onosproject.store.primitives.resources.impl.AtomixWorkQueueCommands.Register;
import org.onosproject.store.primitives.resources.impl.AtomixWorkQueueCommands.Stats;
import org.onosproject.store.primitives.resources.impl.AtomixWorkQueueCommands.Take;
import org.onosproject.store.primitives.resources.impl.AtomixWorkQueueCommands.Unregister;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.Task;
import org.onosproject.store.service.WorkQueueStats;
import org.slf4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

/**
 * State machine for {@link AtomixWorkQueue} resource.
 */
public class AtomixWorkQueueState extends StateMachine implements SessionListener, Snapshottable {

    private final Logger log = getLogger(getClass());
    private final Serializer serializer = Serializer.using(KryoNamespace.newBuilder()
            .register(KryoNamespaces.BASIC)
            .register(TaskAssignment.class)
            .register(Task.class)
            .register(new HashMap().keySet().getClass())
            .register(ArrayDeque.class)
            .build());

    private final AtomicLong totalCompleted = new AtomicLong(0);

    private final Queue<Task<byte[]>> unassignedTasks = Queues.newArrayDeque();
    private final Map<String, TaskAssignment> assignments = Maps.newHashMap();
    private final Map<Long, ServerSession> registeredWorkers = Maps.newHashMap();

    @Override
    public void snapshot(SnapshotWriter writer) {
        byte[] registeredWorkersBytes = serializer.encode(registeredWorkers.keySet());
        writer.writeInt(registeredWorkersBytes.length);
        writer.write(registeredWorkersBytes);

        byte[] assignmentsBytes = serializer.encode(assignments);
        writer.writeInt(assignmentsBytes.length);
        writer.write(assignmentsBytes);

        byte[] unassignedTasksBytes = serializer.encode(unassignedTasks);
        writer.writeInt(unassignedTasksBytes.length);
        writer.write(unassignedTasksBytes);

        writer.writeLong(totalCompleted.get());
    }

    @Override
    public void install(SnapshotReader reader) {
        registeredWorkers.clear();
        int registeredWorkersLength = reader.readInt();
        byte[] registeredWorkersBytes = reader.readBytes(registeredWorkersLength);
        for (long sessionId : serializer.<Set<Long>>decode(registeredWorkersBytes)) {
            registeredWorkers.put(sessionId, sessions.session(sessionId));
        }

        assignments.clear();
        int assignmentsLength = reader.readInt();
        byte[] assignmentsBytes = reader.readBytes(assignmentsLength);
        assignments.putAll(serializer.decode(assignmentsBytes));

        unassignedTasks.clear();
        int unassignedTasksLength = reader.readInt();
        byte[] unassignedTasksBytes = reader.readBytes(unassignedTasksLength);
        unassignedTasks.addAll(serializer.decode(unassignedTasksBytes));

        totalCompleted.set(reader.readLong());
    }

    @Override
    protected void configure(StateMachineExecutor executor) {
        executor.register(Stats.class, this::stats);
        executor.register(Register.class, (Consumer<Commit<Register>>) this::register);
        executor.register(Unregister.class, (Consumer<Commit<Unregister>>) this::unregister);
        executor.register(Add.class, (Consumer<Commit<Add>>) this::add);
        executor.register(Take.class, this::take);
        executor.register(Complete.class, (Consumer<Commit<Complete>>) this::complete);
        executor.register(Clear.class, (Consumer<Commit<Clear>>) this::clear);
    }

    protected WorkQueueStats stats(Commit<? extends Stats> commit) {
        return WorkQueueStats.builder()
                .withTotalCompleted(totalCompleted.get())
                .withTotalPending(unassignedTasks.size())
                .withTotalInProgress(assignments.size())
                .build();
    }

    protected void clear(Commit<? extends Clear> commit) {
        unassignedTasks.clear();
        assignments.clear();
        registeredWorkers.clear();
        totalCompleted.set(0);
    }

    protected void register(Commit<? extends Register> commit) {
        registeredWorkers.put(commit.session().id(), commit.session());
    }

    protected void unregister(Commit<? extends Unregister> commit) {
        registeredWorkers.remove(commit.session().id());
    }

    protected void add(Commit<? extends Add> commit) {
        Collection<byte[]> items = commit.operation().items();

        AtomicInteger itemIndex = new AtomicInteger(0);
        items.forEach(item -> {
            String taskId = String.format("%d:%d:%d", commit.session().id(),
                                                      commit.index(),
                                                      itemIndex.getAndIncrement());
            unassignedTasks.add(new Task<>(taskId, item));
        });

        // Send an event to all sessions that have expressed interest in task processing
        // and are not actively processing a task.
        registeredWorkers.values().forEach(session -> session.publish(AtomixWorkQueue.TASK_AVAILABLE));
        // FIXME: This generates a lot of event traffic.
    }

    protected Collection<Task<byte[]>> take(Commit<? extends Take> commit) {
        try {
            if (unassignedTasks.isEmpty()) {
                return ImmutableList.of();
            }
            long sessionId = commit.session().id();
            int maxTasks = commit.operation().maxTasks();
            return IntStream.range(0, Math.min(maxTasks, unassignedTasks.size()))
                            .mapToObj(i -> {
                                Task<byte[]> task = unassignedTasks.poll();
                                String taskId = task.taskId();
                                TaskAssignment assignment = new TaskAssignment(sessionId, task);

                                // bookkeeping
                                assignments.put(taskId, assignment);

                                return task;
                            })
                            .collect(Collectors.toCollection(ArrayList::new));
        } catch (Exception e) {
            log.warn("State machine update failed", e);
            throw Throwables.propagate(e);
        }
    }

    protected void complete(Commit<? extends Complete> commit) {
        long sessionId = commit.session().id();
        try {
            commit.operation().taskIds().forEach(taskId -> {
                TaskAssignment assignment = assignments.get(taskId);
                if (assignment != null && assignment.sessionId() == sessionId) {
                    assignments.remove(taskId);
                    // bookkeeping
                    totalCompleted.incrementAndGet();
                }
            });
        } catch (Exception e) {
            log.warn("State machine update failed", e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void register(ServerSession session) {
    }

    @Override
    public void unregister(ServerSession session) {
    }

    @Override
    public void expire(ServerSession session) {
    }

    @Override
    public void close(ServerSession session) {
        evictWorker(session.id());
    }

    private void evictWorker(long sessionId) {
        registeredWorkers.remove(sessionId);

        // TODO: Maintain an index of tasks by session for efficient access.
        Iterator<Map.Entry<String, TaskAssignment>> iter = assignments.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, TaskAssignment> entry = iter.next();
            TaskAssignment assignment = entry.getValue();
            if (assignment.sessionId() == sessionId) {
                unassignedTasks.add(assignment.task());
                iter.remove();
            }
        }
    }

    private static class TaskAssignment {
        private final long sessionId;
        private final Task<byte[]> task;

        public TaskAssignment(long sessionId, Task<byte[]> task) {
            this.sessionId = sessionId;
            this.task = task;
        }

        public long sessionId() {
            return sessionId;
        }

        public Task<byte[]> task() {
            return task;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                              .add("sessionId", sessionId)
                              .add("task", task)
                              .toString();
        }
    }
}
