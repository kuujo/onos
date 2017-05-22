/*
 * Copyright 2015-present Open Networking Laboratory
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
package org.onosproject.store.cluster.messaging.impl;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.FutureListener;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.cluster.ClusterMetadataService;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.core.HybridLogicalClockService;
import org.onosproject.store.cluster.messaging.Endpoint;
import org.onosproject.store.cluster.messaging.MessagingException;
import org.onosproject.store.cluster.messaging.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.net.ConnectException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.security.AppGuard.checkPermission;
import static org.onosproject.security.AppPermission.Type.CLUSTER_WRITE;

/**
 * Netty based MessagingService.
 */
@Component(immediate = true)
@Service
public class NettyMessagingManager implements MessagingService {
    private static final long DEFAULT_TIMEOUT_MILLIS = 500;
    private static final long HISTORY_EXPIRE_MILLIS = Duration.ofMinutes(10).toMillis();
    private static final long MIN_TIMEOUT_MILLIS = 100;
    private static final long MAX_TIMEOUT_MILLIS = 5000;
    private static final long TIMEOUT_INTERVAL = 50;
    private static final int WINDOW_SIZE = 100;
    private static final double TIMEOUT_MULTIPLIER = 2.5;
    private static final short MIN_KS_LENGTH = 6;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ClientConnection localClientConnection = new LocalClientConnection();
    private final ServerConnection localServerConnection = new LocalServerConnection(null);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected HybridLogicalClockService clockService;

    private Endpoint localEndpoint;
    private int preamble;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Map<String, BiConsumer<InternalRequest, ServerConnection>> handlers = new ConcurrentHashMap<>();
    private final Map<Channel, RemoteClientConnection> clientConnections = Maps.newConcurrentMap();
    private final Map<Channel, RemoteServerConnection> serverConnections = Maps.newConcurrentMap();
    private final AtomicLong messageIdGenerator = new AtomicLong(0);

    private final Cache<String, TimeoutHistory> timeoutHistories = CacheBuilder.newBuilder()
            .expireAfterAccess(HISTORY_EXPIRE_MILLIS, TimeUnit.MILLISECONDS)
            .build();
    private ScheduledFuture<?> timeoutFuture;

    private final ChannelPoolMap<Endpoint, SimpleChannelPool> channels =
            new AbstractChannelPoolMap<Endpoint, SimpleChannelPool>() {
                @Override
                protected SimpleChannelPool newPool(Endpoint endpoint) {
                    return new SimpleChannelPool(bootstrapClient(endpoint), new ClientChannelPoolHandler());
                }
            };

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private Class<? extends ServerChannel> serverChannelClass;
    private Class<? extends Channel> clientChannelClass;

    protected static final boolean TLS_DISABLED = false;
    protected boolean enableNettyTls = TLS_DISABLED;

    protected String ksLocation;
    protected String tsLocation;
    protected char[] ksPwd;
    protected char[] tsPwd;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterMetadataService clusterMetadataService;

    @Activate
    public void activate() throws Exception {
        ControllerNode localNode = clusterMetadataService.getLocalNode();
        getTlsParameters();

        if (started.get()) {
            log.warn("Already running at local endpoint: {}", localEndpoint);
            return;
        }
        this.preamble = clusterMetadataService.getClusterMetadata().getName().hashCode();
        this.localEndpoint = new Endpoint(localNode.ip(), localNode.tcpPort());
        initEventLoopGroup();
        startAcceptingConnections();
        timeoutFuture = serverGroup.scheduleAtFixedRate(
                this::timeoutCallbacks, TIMEOUT_INTERVAL, TIMEOUT_INTERVAL, TimeUnit.MILLISECONDS);
        started.set(true);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() throws Exception {
        if (started.get()) {
            serverGroup.shutdownGracefully();
            clientGroup.shutdownGracefully();
            timeoutFuture.cancel(false);
            started.set(false);
        }
        log.info("Stopped");
    }

    private void getTlsParameters() {
        String tempString = System.getProperty("enableNettyTLS");
        enableNettyTls = Strings.isNullOrEmpty(tempString) ? TLS_DISABLED : Boolean.parseBoolean(tempString);
        log.info("enableNettyTLS = {}", enableNettyTls);
        if (enableNettyTls) {
            ksLocation = System.getProperty("javax.net.ssl.keyStore");
            if (Strings.isNullOrEmpty(ksLocation)) {
                enableNettyTls = TLS_DISABLED;
                return;
            }
            tsLocation = System.getProperty("javax.net.ssl.trustStore");
            if (Strings.isNullOrEmpty(tsLocation)) {
                enableNettyTls = TLS_DISABLED;
                return;
            }
            ksPwd = System.getProperty("javax.net.ssl.keyStorePassword").toCharArray();
            if (MIN_KS_LENGTH > ksPwd.length) {
                enableNettyTls = TLS_DISABLED;
                return;
            }
            tsPwd = System.getProperty("javax.net.ssl.trustStorePassword").toCharArray();
            if (MIN_KS_LENGTH > tsPwd.length) {
                enableNettyTls = TLS_DISABLED;
                return;
            }
        }
    }

    private void initEventLoopGroup() {
        // try Epoll first and if that does work, use nio.
        try {
            clientGroup = new EpollEventLoopGroup(0, groupedThreads("NettyMessagingEvt", "epollC-%d", log));
            serverGroup = new EpollEventLoopGroup(0, groupedThreads("NettyMessagingEvt", "epollS-%d", log));
            serverChannelClass = EpollServerSocketChannel.class;
            clientChannelClass = EpollSocketChannel.class;
            return;
        } catch (Throwable e) {
            log.debug("Failed to initialize native (epoll) transport. "
                    + "Reason: {}. Proceeding with nio.", e.getMessage());
        }
        clientGroup = new NioEventLoopGroup(0, groupedThreads("NettyMessagingEvt", "nioC-%d", log));
        serverGroup = new NioEventLoopGroup(0, groupedThreads("NettyMessagingEvt", "nioS-%d", log));
        serverChannelClass = NioServerSocketChannel.class;
        clientChannelClass = NioSocketChannel.class;
    }

    /**
     * Times out response callbacks.
     */
    private synchronized void timeoutCallbacks() {
        // Iterate through all connections and time out callbacks.
        for (RemoteClientConnection connection : clientConnections.values()) {
            connection.timeoutCallbacks();
        }

        // Iterate through all timeout histories and recompute the timeout.
        for (TimeoutHistory timeoutHistory : timeoutHistories.asMap().values()) {
            timeoutHistory.recomputeTimeoutMillis();
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(Endpoint ep, String type, byte[] payload) {
        checkPermission(CLUSTER_WRITE);
        InternalRequest message = new InternalRequest(preamble,
                clockService.timeNow(),
                messageIdGenerator.incrementAndGet(),
                localEndpoint,
                type,
                payload);
        return executeOnPooledConnection(ep, c -> c.sendAsync(message), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload) {
        checkPermission(CLUSTER_WRITE);
        return sendAndReceive(ep, type, payload, MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload, Executor executor) {
        checkPermission(CLUSTER_WRITE);
        Long messageId = messageIdGenerator.incrementAndGet();
        InternalRequest message = new InternalRequest(preamble,
                clockService.timeNow(),
                messageId,
                localEndpoint,
                type,
                payload);
        return executeOnPooledConnection(ep, c -> c.sendAndReceive(message), executor);
    }

    /**
     * Executes the given callback on a pooled connection.
     *
     * @param endpoint the endpoint to which to send a message
     * @param callback the callback to execute to send the message
     * @param <T> the send result type
     * @return a completable future to be completed with the result of the supplied function
     */
    private <T> CompletableFuture<T> executeOnPooledConnection(
            Endpoint endpoint,
            Function<ClientConnection, CompletableFuture<T>> callback,
            Executor executor) {
        if (endpoint.equals(localEndpoint)) {
            CompletableFuture<T> future = new CompletableFuture<>();
            callback.apply(localClientConnection).whenComplete((result, error) -> {
               if (error == null) {
                   executor.execute(() -> future.complete(result));
               } else {
                   executor.execute(() -> future.completeExceptionally(error));
               }
            });
            return future;
        }

        CompletableFuture<T> future = new CompletableFuture<>();
        ChannelPool pool = channels.get(endpoint);
        pool.acquire().addListener((FutureListener<Channel>) channelResult -> {
            if (channelResult.isSuccess()) {
                Channel channel = channelResult.getNow();
                ClientConnection connection = clientConnections.computeIfAbsent(channel, RemoteClientConnection::new);
                callback.apply(connection).whenComplete((result, error) -> {
                    pool.release(channel).addListener(releaseResult -> {
                        if (!releaseResult.isSuccess()) {
                            clientConnections.remove(channel);
                            connection.close();
                        }
                    });

                    if (error == null) {
                        executor.execute(() -> future.complete(result));
                    } else {
                        executor.execute(() -> future.completeExceptionally(error));
                    }
                });
            } else {
                executor.execute(() -> future.completeExceptionally(channelResult.cause()));
            }
        });
        return future;
    }

    @Override
    public void registerHandler(String type, BiConsumer<Endpoint, byte[]> handler, Executor executor) {
        checkPermission(CLUSTER_WRITE);
        handlers.put(type, (message, connection) -> executor.execute(() ->
                handler.accept(message.sender(), message.payload())));
    }

    @Override
    public void registerHandler(String type, BiFunction<Endpoint, byte[], byte[]> handler, Executor executor) {
        checkPermission(CLUSTER_WRITE);
        handlers.put(type, (message, connection) -> executor.execute(() -> {
            byte[] responsePayload = null;
            InternalReply.Status status = InternalReply.Status.OK;
            try {
                responsePayload = handler.apply(message.sender(), message.payload());
            } catch (Exception e) {
                status = InternalReply.Status.ERROR_HANDLER_EXCEPTION;
            }

            if (responsePayload == null) {
                responsePayload = new byte[0];
            }

            InternalReply response = new InternalReply(preamble,
                    clockService.timeNow(),
                    message.id(),
                    responsePayload,
                    status);
            connection.sendReply(response);
        }));
    }

    @Override
    public void registerHandler(String type, BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> handler) {
        checkPermission(CLUSTER_WRITE);
        handlers.put(type, (message, connection) -> {
            handler.apply(message.sender(), message.payload()).whenComplete((result, error) -> {
                InternalReply.Status status = error == null
                        ? InternalReply.Status.OK : InternalReply.Status.ERROR_HANDLER_EXCEPTION;
                if (result == null) {
                    result = new byte[0];
                }
                InternalReply response = new InternalReply(preamble,
                        clockService.timeNow(),
                        message.id(),
                        result,
                        status);
                connection.sendReply(response);
            });
        });
    }

    @Override
    public void unregisterHandler(String type) {
        checkPermission(CLUSTER_WRITE);
        handlers.remove(type);
    }

    private Bootstrap bootstrapClient(Endpoint endpoint) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(10 * 32 * 1024, 10 * 64 * 1024));
        bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);
        bootstrap.group(clientGroup);
        // TODO: Make this faster:
        // http://normanmaurer.me/presentations/2014-facebook-eng-netty/slides.html#37.0
        bootstrap.channel(clientChannelClass);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.remoteAddress(endpoint.host().toInetAddress(), endpoint.port());
        return bootstrap;
    }

    private void startAcceptingConnections() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(8 * 1024, 32 * 1024));
        b.option(ChannelOption.SO_RCVBUF, 1048576);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        b.group(serverGroup, clientGroup);
        b.channel(serverChannelClass);
        if (enableNettyTls) {
            b.childHandler(new SslServerCommunicationChannelInitializer());
        } else {
            b.childHandler(new BasicChannelInitializer());
        }
        b.option(ChannelOption.SO_BACKLOG, 128);
        b.childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.
        b.bind(localEndpoint.port()).sync().addListener(future -> {
            if (future.isSuccess()) {
                log.info("{} accepting incoming connections on port {}",
                        localEndpoint.host(), localEndpoint.port());
            } else {
                log.warn("{} failed to bind to port {} due to {}",
                        localEndpoint.host(), localEndpoint.port(), future.cause());
            }
        });
    }

    /**
     * Channel pool handler.
     */
    private class ClientChannelPoolHandler extends AbstractChannelPoolHandler {
        @Override
        public void channelCreated(Channel channel) throws Exception {
            if (enableNettyTls) {
                new SslClientCommunicationChannelInitializer().initChannel((SocketChannel) channel);
            } else {
                new BasicChannelInitializer().initChannel((SocketChannel) channel);
            }
        }
    }

    /**
     * Channel initializer for TLS servers.
     */
    private class SslServerCommunicationChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ChannelHandler dispatcher = new InboundMessageDispatcher();
        private final ChannelHandler encoder = new MessageEncoder(preamble);

        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(new FileInputStream(tsLocation), tsPwd);
            tmFactory.init(ts);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(new FileInputStream(ksLocation), ksPwd);
            kmf.init(ks, ksPwd);

            SSLContext serverContext = SSLContext.getInstance("TLS");
            serverContext.init(kmf.getKeyManagers(), tmFactory.getTrustManagers(), null);

            SSLEngine serverSslEngine = serverContext.createSSLEngine();

            serverSslEngine.setNeedClientAuth(true);
            serverSslEngine.setUseClientMode(false);
            serverSslEngine.setEnabledProtocols(serverSslEngine.getSupportedProtocols());
            serverSslEngine.setEnabledCipherSuites(serverSslEngine.getSupportedCipherSuites());
            serverSslEngine.setEnableSessionCreation(true);

            channel.pipeline().addLast("ssl", new io.netty.handler.ssl.SslHandler(serverSslEngine))
                    .addLast("encoder", encoder)
                    .addLast("decoder", new MessageDecoder())
                    .addLast("handler", dispatcher);
        }
    }

    /**
     * Channel initializer for TLS clients.
     */
    private class SslClientCommunicationChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ChannelHandler dispatcher = new InboundMessageDispatcher();
        private final ChannelHandler encoder = new MessageEncoder(preamble);

        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(new FileInputStream(tsLocation), tsPwd);
            tmFactory.init(ts);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(new FileInputStream(ksLocation), ksPwd);
            kmf.init(ks, ksPwd);

            SSLContext clientContext = SSLContext.getInstance("TLS");
            clientContext.init(kmf.getKeyManagers(), tmFactory.getTrustManagers(), null);

            SSLEngine clientSslEngine = clientContext.createSSLEngine();

            clientSslEngine.setUseClientMode(true);
            clientSslEngine.setEnabledProtocols(clientSslEngine.getSupportedProtocols());
            clientSslEngine.setEnabledCipherSuites(clientSslEngine.getSupportedCipherSuites());
            clientSslEngine.setEnableSessionCreation(true);

            channel.pipeline().addLast("ssl", new io.netty.handler.ssl.SslHandler(clientSslEngine))
                    .addLast("encoder", encoder)
                    .addLast("decoder", new MessageDecoder())
                    .addLast("handler", dispatcher);
        }
    }

    /**
     * Channel initializer for basic connections.
     */
    private class BasicChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ChannelHandler dispatcher = new InboundMessageDispatcher();
        private final ChannelHandler encoder = new MessageEncoder(preamble);

        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            channel.pipeline()
                    .addLast("encoder", encoder)
                    .addLast("decoder", new MessageDecoder())
                    .addLast("handler", dispatcher);
        }
    }

    /**
     * Channel inbound handler that dispatches messages to the appropriate handler.
     */
    @ChannelHandler.Sharable
    private class InboundMessageDispatcher extends SimpleChannelInboundHandler<Object> {
        // Effectively SimpleChannelInboundHandler<InternalMessage>,
        // had to specify <Object> to avoid Class Loader not being able to find some classes.

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object rawMessage) throws Exception {
            InternalMessage message = (InternalMessage) rawMessage;
            try {
                if (message.type() == InternalMessage.Type.REQUEST) {
                    RemoteServerConnection connection =
                            serverConnections.computeIfAbsent(ctx.channel(), RemoteServerConnection::new);
                    connection.dispatch((InternalRequest) message);
                } else {
                    RemoteClientConnection connection =
                            clientConnections.computeIfAbsent(ctx.channel(), RemoteClientConnection::new);
                    connection.dispatch((InternalReply) message);
                }
            } catch (RejectedExecutionException e) {
                log.warn("Unable to dispatch message due to {}", e.getMessage());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            log.error("Exception inside channel handling pipeline.", cause);

            RemoteClientConnection clientConnection = clientConnections.remove(context.channel());
            if (clientConnection != null) {
                clientConnection.close();
            }

            RemoteServerConnection serverConnection = serverConnections.remove(context.channel());
            if (serverConnection != null) {
                serverConnection.close();
            }
            context.close();
        }

        /**
         * Returns true if the given message should be handled.
         *
         * @param msg inbound message
         * @return true if {@code msg} is {@link InternalMessage} instance.
         *
         * @see SimpleChannelInboundHandler#acceptInboundMessage(Object)
         */
        @Override
        public final boolean acceptInboundMessage(Object msg) {
            return msg instanceof InternalMessage;
        }
    }

    /**
     * Wraps a {@link CompletableFuture} and tracks its type and creation time.
     */
    private final class Callback {
        private final String subject;
        private final CompletableFuture<byte[]> future;
        private final long time = System.currentTimeMillis();

        Callback(String subject, CompletableFuture<byte[]> future) {
            this.subject = subject;
            this.future = future;
        }

        public void complete(byte[] value) {
            future.complete(value);
        }

        public void completeExceptionally(Throwable error) {
            future.completeExceptionally(error);
        }
    }

    /**
     * Represents the client side of a connection to a local or remote server.
     */
    private interface ClientConnection {

        /**
         * Sends a message to the other side of the connection.
         *
         * @param message the message to send
         * @return a completable future to be completed once the message has been sent
         */
        CompletableFuture<Void> sendAsync(InternalRequest message);

        /**
         * Sends a message to the other side of the connection, awaiting a reply.
         *
         * @param message the message to send
         * @return a completable future to be completed once a reply is received or the request times out
         */
        CompletableFuture<byte[]> sendAndReceive(InternalRequest message);

        /**
         * Closes the connection.
         */
        default void close() {
        }
    }

    /**
     * Represents the server side of a connection.
     */
    private interface ServerConnection {

        /**
         * Sends a reply to the other side of the connection.
         *
         * @param message the message to send
         */
        void sendReply(InternalReply message);

        /**
         * Closes the connection.
         */
        default void close() {
        }
    }

    /**
     * Local connection implementation.
     */
    private final class LocalClientConnection implements ClientConnection {
        @Override
        public CompletableFuture<Void> sendAsync(InternalRequest message) {
            BiConsumer<InternalRequest, ServerConnection> handler = handlers.get(message.subject());
            if (handler != null) {
                handler.accept(message, localServerConnection);
            } else {
                log.debug("No handler for message type {} from {}", message.subject(), message.sender());
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<byte[]> sendAndReceive(InternalRequest message) {
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            BiConsumer<InternalRequest, ServerConnection> handler = handlers.get(message.subject());
            LocalServerConnection connection = new LocalServerConnection(future);
            if (handler != null) {
                handler.accept(message, connection);
            } else {
                log.debug("No handler for message type {} from {}", message.subject(), message.sender());
                InternalReply response = new InternalReply(preamble,
                        clockService.timeNow(),
                        message.id(),
                        new byte[0],
                        InternalReply.Status.ERROR_NO_HANDLER);
                connection.sendReply(response);
            }
            return future;
        }
    }

    /**
     * Local server connection.
     */
    private final class LocalServerConnection implements ServerConnection {
        private final CompletableFuture<byte[]> future;

        LocalServerConnection(CompletableFuture<byte[]> future) {
            this.future = future;
        }

        @Override
        public void sendReply(InternalReply message) {
            if (future != null) {
                if (message.status() == InternalReply.Status.OK) {
                    future.complete(message.payload());
                } else if (message.status() == InternalReply.Status.ERROR_NO_HANDLER) {
                    future.completeExceptionally(new MessagingException.NoRemoteHandler());
                } else if (message.status() == InternalReply.Status.ERROR_HANDLER_EXCEPTION) {
                    future.completeExceptionally(new MessagingException.RemoteHandlerFailure());
                } else if (message.status() == InternalReply.Status.PROTOCOL_EXCEPTION) {
                    future.completeExceptionally(new MessagingException.ProtocolException());
                }
            }
        }
    }

    /**
     * Remote connection implementation.
     */
    private final class RemoteClientConnection implements ClientConnection {
        private final Channel channel;
        private final Map<Long, Callback> futures = Maps.newConcurrentMap();
        private final ScheduledFuture<?> timeoutFuture;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        RemoteClientConnection(Channel channel) {
            this.channel = channel;
            timeoutFuture = serverGroup.scheduleAtFixedRate(
                    this::timeoutCallbacks, TIMEOUT_INTERVAL, TIMEOUT_INTERVAL, TimeUnit.MILLISECONDS);
        }

        /**
         * Times out callbacks.
         */
        private void timeoutCallbacks() {
            // Store the current time.
            long currentTime = System.currentTimeMillis();

            // Iterate through future callbacks and time out callbacks that have been alive
            // longer than the current timeout according to the message type.
            Iterator<Map.Entry<Long, Callback>> iterator = futures.entrySet().iterator();
            while (iterator.hasNext()) {
                Callback callback = iterator.next().getValue();
                try {
                    TimeoutHistory timeoutHistory = timeoutHistories.get(callback.subject, TimeoutHistory::new);
                    long currentTimeout = timeoutHistory.currentTimeout;
                    if (currentTime - callback.time > currentTimeout) {
                        iterator.remove();
                        long elapsedTime = currentTime - callback.time;
                        timeoutHistory.addReplyTime(elapsedTime);
                        callback.completeExceptionally(
                                new TimeoutException("Request timed out in " + elapsedTime + " milliseconds"));
                    }
                } catch (ExecutionException e) {
                    throw new AssertionError();
                }
            }
        }

        @Override
        public CompletableFuture<Void> sendAsync(InternalRequest message) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            channel.writeAndFlush(message).addListener(channelFuture -> {
                if (!channelFuture.isSuccess()) {
                    future.completeExceptionally(channelFuture.cause());
                } else {
                    future.complete(null);
                }
            });
            return future;
        }

        @Override
        public CompletableFuture<byte[]> sendAndReceive(InternalRequest message) {
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            Callback callback = new Callback(message.subject(), future);
            futures.put(message.id(), callback);
            channel.writeAndFlush(message).addListener(channelFuture -> {
                if (!channelFuture.isSuccess()) {
                    futures.remove(message.id());
                    callback.completeExceptionally(channelFuture.cause());
                }
            });
            return future;
        }

        /**
         * Dispatches a message to a local handler.
         *
         * @param message the message to dispatch
         */
        private void dispatch(InternalReply message) {
            if (message.preamble() != preamble) {
                log.debug("Received reply with invalid preamble");
                return;
            }

            clockService.recordEventTime(message.time());

            Callback callback = futures.remove(message.id());
            if (callback != null) {
                if (message.status() == InternalReply.Status.OK) {
                    callback.complete(message.payload());
                } else if (message.status() == InternalReply.Status.ERROR_NO_HANDLER) {
                    callback.completeExceptionally(new MessagingException.NoRemoteHandler());
                } else if (message.status() == InternalReply.Status.ERROR_HANDLER_EXCEPTION) {
                    callback.completeExceptionally(new MessagingException.RemoteHandlerFailure());
                } else if (message.status() == InternalReply.Status.PROTOCOL_EXCEPTION) {
                    callback.completeExceptionally(new MessagingException.ProtocolException());
                }

                try {
                    TimeoutHistory timeoutHistory = timeoutHistories.get(callback.subject, TimeoutHistory::new);
                    timeoutHistory.addReplyTime(System.currentTimeMillis() - callback.time);
                } catch (ExecutionException e) {
                    throw new AssertionError();
                }
            } else {
                log.debug("Received a reply for message id:[{}]"
                        + " but was unable to locate the"
                        + " request handle", message.id());
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                timeoutFuture.cancel(false);
                for (Callback callback : futures.values()) {
                    callback.completeExceptionally(new ConnectException());
                }
            }
        }
    }

    /**
     * Remote server connection.
     */
    private final class RemoteServerConnection implements ServerConnection {
        private final Channel channel;

        RemoteServerConnection(Channel channel) {
            this.channel = channel;
        }

        /**
         * Dispatches a message to a local handler.
         *
         * @param message the message to dispatch
         */
        private void dispatch(InternalRequest message) {
            if (message.preamble() != preamble) {
                log.debug("Received request with invalid preamble from {}", message.sender());
                InternalReply response = new InternalReply(preamble,
                        clockService.timeNow(),
                        message.id(),
                        new byte[0],
                        InternalReply.Status.PROTOCOL_EXCEPTION);
                sendReply(response);
                return;
            }

            clockService.recordEventTime(message.time());

            BiConsumer<InternalRequest, ServerConnection> handler = handlers.get(message.subject());
            if (handler != null) {
                handler.accept(message, this);
            } else {
                log.debug("No handler for message type {} from {}", message.subject(), message.sender());
                InternalReply response = new InternalReply(preamble,
                        clockService.timeNow(),
                        message.id(),
                        new byte[0],
                        InternalReply.Status.ERROR_NO_HANDLER);
                sendReply(response);
            }
        }

        @Override
        public void sendReply(InternalReply message) {
            channel.writeAndFlush(message);
        }
    }

    /**
     * Request-reply timeout history tracker.
     * <p>
     * This class is not thread safe.
     */
    private static final class TimeoutHistory {
        private final DescriptiveStatistics timeoutHistory = new SynchronizedDescriptiveStatistics(WINDOW_SIZE);
        private long maxReplyTime = 0;
        private long currentTimeout = DEFAULT_TIMEOUT_MILLIS;

        /**
         * Adds a reply time to the history.
         *
         * @param replyTime the reply time to add to the history
         */
        void addReplyTime(long replyTime) {
            this.maxReplyTime = Math.max(this.maxReplyTime, replyTime);
        }

        /**
         * Computes the current timeout.
         */
        private void recomputeTimeoutMillis() {
            timeoutHistory.addValue(
                    Math.min(Math.max(maxReplyTime * TIMEOUT_MULTIPLIER, MIN_TIMEOUT_MILLIS), MAX_TIMEOUT_MILLIS));
            if (timeoutHistory.getN() == WINDOW_SIZE) {
                this.currentTimeout = (long) timeoutHistory.getMax();
            }
            this.maxReplyTime = 0;
        }
    }
}
