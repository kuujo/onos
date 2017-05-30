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
package org.onosproject.store.primitives.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Throwables;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.Listeners;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.SerializationException;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.TransportException;
import io.atomix.catalyst.util.reference.ReferenceCounted;
import org.onlab.util.Tools;
import org.onosproject.cluster.PartitionId;
import org.onosproject.store.cluster.messaging.Endpoint;
import org.onosproject.store.cluster.messaging.MessagingException;
import org.onosproject.store.cluster.messaging.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.onosproject.store.primitives.impl.CopycatTransport.FAILURE;
import static org.onosproject.store.primitives.impl.CopycatTransport.SUCCESS;

/**
 * Base Copycat Transport connection.
 */
public class CopycatTransportConnection implements Connection {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final long connectionId;
    private final String localSubject;
    private final String remoteSubject;
    private final PartitionId partitionId;
    private final Endpoint endpoint;
    private final MessagingService messagingService;
    private final ThreadContext context;
    private final Listeners<Throwable> exceptionListeners = new Listeners<>();
    private final Listeners<Connection> closeListeners = new Listeners<>();
    private final Set<String> registeredSubjects = new CopyOnWriteArraySet<>();

    CopycatTransportConnection(
            long connectionId,
            Mode mode,
            PartitionId partitionId,
            Endpoint endpoint,
            MessagingService messagingService,
            ThreadContext context) {
        this.connectionId = connectionId;
        this.partitionId = checkNotNull(partitionId, "partitionId cannot be null");
        this.localSubject = mode.getLocalSubject(partitionId, connectionId);
        this.remoteSubject = mode.getRemoteSubject(partitionId, connectionId);
        this.endpoint = checkNotNull(endpoint, "endpoint cannot be null");
        this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
        this.context = checkNotNull(context, "context cannot be null");
        messagingService.registerHandler(localSubject, (e, m) -> handleClose());
    }

    /**
     * Returns the local subject for the given message type.
     *
     * @param type the message type for which to return the local subject
     * @return the local subject for the given message type
     */
    private String localType(String type) {
        return localSubject + "-" + type;
    }

    /**
     * Returns the remote subject for the given message type.
     *
     * @param type the message type for which to return the remote subject
     * @return the remote subject for the given message type
     */
    private String remoteType(String type) {
        return remoteSubject + "-" + type;
    }

    @Override
    public CompletableFuture<Void> send(String type, Object message) {
        ThreadContext context = ThreadContext.currentContextOrThrow();
        CompletableFuture<Void> future = new CompletableFuture<>();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            context.serializer().writeObject(message, new DataOutputStream(baos));
            if (message instanceof ReferenceCounted) {
                ((ReferenceCounted<?>) message).release();
            }

            messagingService.sendAsync(endpoint, remoteType(type), baos.toByteArray())
                    .whenComplete((r, e) -> {
                        if (e != null) {
                            context.execute(() -> future.completeExceptionally(e));
                        } else {
                            context.execute(() -> future.complete(null));
                        }
                    });
        } catch (SerializationException | IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public <T, U> CompletableFuture<U> sendAndReceive(String type, T message) {
        ThreadContext context = ThreadContext.currentContextOrThrow();
        CompletableFuture<U> future = new CompletableFuture<>();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            context.serializer().writeObject(message, new DataOutputStream(baos));
            if (message instanceof ReferenceCounted) {
                ((ReferenceCounted<?>) message).release();
            }
            messagingService.sendAndReceive(endpoint, remoteType(type), baos.toByteArray(), context)
                    .whenComplete((response, error) -> handleResponse(response, error, future));
        } catch (SerializationException | IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * Handles a response received from the other side of the connection.
     */
    private <T> void handleResponse(
            byte[] response,
            Throwable error,
            CompletableFuture<T> future) {
        if (error != null) {
            Throwable rootCause = Throwables.getRootCause(error);
            if (rootCause instanceof MessagingException || rootCause instanceof SocketException) {
                future.completeExceptionally(new TransportException(error));
                if (rootCause instanceof MessagingException.NoRemoteHandler) {
                    close(rootCause);
                }
            } else {
                future.completeExceptionally(error);
            }
            return;
        }

        checkNotNull(response);
        InputStream input = new ByteArrayInputStream(response);
        try {
            byte status = (byte) input.read();
            if (status == FAILURE) {
                Throwable t = context.serializer().readObject(input);
                future.completeExceptionally(t);
            } else {
                try {
                    future.complete(context.serializer().readObject(input));
                } catch (SerializationException e) {
                    future.completeExceptionally(e);
                }
            }
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
    }

    /**
     * Handles a message from the other side of the connection.
     */
    @SuppressWarnings("unchecked")
    private CompletableFuture<byte[]> handleMessage(
            byte[] message,
            Function<Object, CompletableFuture<Object>> handler,
            ThreadContext context) {
        try {
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            context.execute(() -> {
                Object request = context.serializer().readObject(new ByteArrayInputStream(message));
                CompletableFuture<Object> responseFuture = handler.apply(request);
                if (responseFuture != null) {
                    responseFuture.whenComplete((response, error) -> {
                        if (error != null) {
                            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                                baos.write(FAILURE);
                                context.serializer().writeObject(error, baos);
                                future.complete(baos.toByteArray());
                            } catch (IOException e) {
                                Throwables.propagate(e);
                            }
                        } else {
                            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                                baos.write(SUCCESS);
                                context.serializer().writeObject(response, baos);
                                future.complete(baos.toByteArray());
                            } catch (IOException e) {
                                Throwables.propagate(e);
                            }
                        }
                    });
                }
            });
            return future;
        } catch (Exception e) {
            return Tools.exceptionalFuture(e);
        }
    }

    /**
     * Handles a close request from the other side of the connection.
     */
    private CompletableFuture<byte[]> handleClose() {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        context.execute(() -> {
            close(null);
            ByteBuffer responseBuffer = ByteBuffer.allocate(1);
            responseBuffer.put(SUCCESS);
            future.complete(responseBuffer.array());
        });
        return future;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Connection registerHandler(String type, Consumer<T> handler) {
        return registerHandler(type, r -> {
            handler.accept((T) r);
            return null;
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, U> Connection registerHandler(String type, Function<T, CompletableFuture<U>> handler) {
        String subject = localType(type);
        ThreadContext context = ThreadContext.currentContextOrThrow();
        messagingService.registerHandler(subject,
                (e, m) -> handleMessage(m, (Function) handler, context));
        if (log.isTraceEnabled()) {
            log.trace("Registered handler on connection {}-{}: {}", partitionId, connectionId, type);
        }
        registeredSubjects.add(subject);
        return this;
    }

    @Override
    public Listener<Throwable> onException(Consumer<Throwable> consumer) {
        return exceptionListeners.add(consumer);
    }

    @Override
    public Listener<Connection> onClose(Consumer<Connection> consumer) {
        return closeListeners.add(consumer);
    }

    @Override
    public CompletableFuture<Void> close() {
        log.debug("Closing connection {}-{}", partitionId, connectionId);

        ThreadContext context = ThreadContext.currentContextOrThrow();
        CompletableFuture<Void> future = new CompletableFuture<>();
        messagingService.sendAndReceive(endpoint, remoteSubject, new byte[0], context)
                .whenComplete((payload, error) -> {
                    close(error);
                    Throwable wrappedError = error;
                    if (error != null) {
                        Throwable rootCause = Throwables.getRootCause(error);
                        if (MessagingException.class.isAssignableFrom(rootCause.getClass())) {
                            wrappedError = new TransportException(error);
                        }
                        future.completeExceptionally(wrappedError);
                    } else {
                        ByteBuffer responseBuffer = ByteBuffer.wrap(payload);
                        if (responseBuffer.get() == SUCCESS) {
                            future.complete(null);
                        } else {
                            future.completeExceptionally(new TransportException("Failed to close connection"));
                        }
                    }
                });
        return future;
    }

    /**
     * Cleans up the connection, unregistering handlers registered on the MessagingService.
     */
    private void close(Throwable error) {
        log.debug("Connection {}-{} closed", partitionId, connectionId);
        for (String subject : registeredSubjects) {
            messagingService.unregisterHandler(subject);
        }
        messagingService.unregisterHandler(localSubject);
        if (error != null) {
            exceptionListeners.accept(error);
        }
        closeListeners.accept(this);
    }

    /**
     * Connection mode used to indicate whether this side of the connection is
     * a client or server.
     */
    enum Mode {

        /**
         * Represents the client side of a bi-directional connection.
         */
        CLIENT {
            @Override
            String getLocalSubject(PartitionId partitionId, long connectionId) {
                return String.format("onos-copycat-%s-%d-client", partitionId, connectionId);
            }

            @Override
            String getRemoteSubject(PartitionId partitionId, long connectionId) {
                return String.format("onos-copycat-%s-%d-server", partitionId, connectionId);
            }
        },

        /**
         * Represents the server side of a bi-directional connection.
         */
        SERVER {
            @Override
            String getLocalSubject(PartitionId partitionId, long connectionId) {
                return String.format("onos-copycat-%s-%d-server", partitionId, connectionId);
            }

            @Override
            String getRemoteSubject(PartitionId partitionId, long connectionId) {
                return String.format("onos-copycat-%s-%d-client", partitionId, connectionId);
            }
        };

        /**
         * Returns the local messaging service subject for the connection in this mode.
         * Subjects generated by the connection mode are guaranteed to be globally unique.
         *
         * @param partitionId the partition ID to which the connection belongs.
         * @param connectionId the connection ID.
         * @return the globally unique local subject for the connection.
         */
        abstract String getLocalSubject(PartitionId partitionId, long connectionId);

        /**
         * Returns the remote messaging service subject for the connection in this mode.
         * Subjects generated by the connection mode are guaranteed to be globally unique.
         *
         * @param partitionId the partition ID to which the connection belongs.
         * @param connectionId the connection ID.
         * @return the globally unique remote subject for the connection.
         */
        abstract String getRemoteSubject(PartitionId partitionId, long connectionId);
    }

    /**
     * Internal container for a handler/context pair.
     */
    private static class InternalHandler {
        private final Function handler;
        private final ThreadContext context;

        InternalHandler(Function handler, ThreadContext context) {
            this.handler = handler;
            this.context = context;
        }

        @SuppressWarnings("unchecked")
        CompletableFuture<Object> handle(Object message) {
            CompletableFuture<Object> future = new CompletableFuture<>();
            context.execute(() -> {
                CompletableFuture<Object> responseFuture = (CompletableFuture<Object>) handler.apply(message);
                if (responseFuture != null) {
                    responseFuture.whenComplete((r, e) -> {
                        if (e != null) {
                            future.completeExceptionally((Throwable) e);
                        } else {
                            future.complete(r);
                        }
                    });
                }
            });
            return future;
        }
    }
}
