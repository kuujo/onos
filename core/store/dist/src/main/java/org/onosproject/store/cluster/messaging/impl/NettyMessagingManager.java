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
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onlab.util.Tools;
import org.onosproject.cluster.ClusterMetadataService;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.core.HybridLogicalClockService;
import org.onosproject.store.cluster.messaging.Endpoint;
import org.onosproject.store.cluster.messaging.MessagingException;
import org.onosproject.store.cluster.messaging.MessagingService;
import org.onosproject.store.cluster.messaging.impl.InternalMessage.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.security.AppGuard.checkPermission;
import static org.onosproject.security.AppPermission.Type.CLUSTER_WRITE;

/**
 * Netty based MessagingService.
 */
@Component(immediate = true)
@Service
public class NettyMessagingManager implements MessagingService {

    private static final int DEFAULT_TIMEOUT_MILLIS = 500;
    private static final long MIN_TIMEOUT_MILLIS = 100;
    private static final int WINDOW_SIZE = 100;
    private static final int MIN_SAMPLES = 10;
    private static final int TIMEOUT_PERCENTILE = 90;
    private static final double TIMEOUT_MULTIPLIER = 1.5;
    private static final short MIN_KS_LENGTH = 6;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final String REPLY_MESSAGE_TYPE = "NETTY_MESSAGING_REQUEST_REPLY";

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected HybridLogicalClockService clockService;

    private Endpoint localEp;
    private int preamble;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Map<String, BiConsumer<InternalMessage, Connection>> handlers = new ConcurrentHashMap<>();
    private final Map<Channel, Connection> connections = Maps.newConcurrentMap();
    private final AtomicLong messageIdGenerator = new AtomicLong(0);

    private final GenericKeyedObjectPool<Endpoint, Connection> channels
            = new GenericKeyedObjectPool<>(new OnosCommunicationChannelFactory());

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
            log.warn("Already running at local endpoint: {}", localEp);
            return;
        }
        this.preamble = clusterMetadataService.getClusterMetadata().getName().hashCode();
        this.localEp = new Endpoint(localNode.ip(), localNode.tcpPort());
        channels.setLifo(true);
        channels.setTestOnBorrow(true);
        channels.setTestOnReturn(true);
        channels.setMinEvictableIdleTimeMillis(60_000L);
        channels.setTimeBetweenEvictionRunsMillis(30_000L);
        initEventLoopGroup();
        startAcceptingConnections();
        started.set(true);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() throws Exception {
        if (started.get()) {
            channels.close();
            serverGroup.shutdownGracefully();
            clientGroup.shutdownGracefully();
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

    @Override
    public CompletableFuture<Void> sendAsync(Endpoint ep, String type, byte[] payload) {
        checkPermission(CLUSTER_WRITE);
        InternalMessage message = new InternalMessage(preamble,
                                                      clockService.timeNow(),
                                                      messageIdGenerator.incrementAndGet(),
                                                      localEp,
                                                      type,
                                                      payload);
        return sendAsync(ep, message, null);
    }

    protected CompletableFuture<Void> sendAsync(Endpoint ep, InternalMessage message, Callback callback) {
        checkPermission(CLUSTER_WRITE);
        if (ep.equals(localEp)) {
            try {
                dispatchLocally(message, null);
            } catch (IOException e) {
                return Tools.exceptionalFuture(e);
            }
            return CompletableFuture.completedFuture(null);
        }

        try {
            Connection connection = null;
            try {
                connection = channels.borrowObject(ep);
                return connection.send(message, callback);
            } finally {
                if (connection != null) {
                    channels.returnObject(ep, connection);
                }
            }
        } catch (Exception e) {
            return Tools.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload) {
        checkPermission(CLUSTER_WRITE);
        return sendAndReceive(ep, type, payload, MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload, Executor executor) {
        checkPermission(CLUSTER_WRITE);
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        Callback callback = new Callback(future, executor);
        Long messageId = messageIdGenerator.incrementAndGet();
        InternalMessage message = new InternalMessage(preamble,
                                                      clockService.timeNow(),
                                                      messageId,
                                                      localEp,
                                                      type,
                                                      payload);

        sendAsync(ep, message, callback).whenComplete((response, error) -> {
            if (error != null) {
                callback.completeExceptionally(error);
            }
        });
        return future;
    }

    @Override
    public void registerHandler(String type, BiConsumer<Endpoint, byte[]> handler, Executor executor) {
        checkPermission(CLUSTER_WRITE);
        handlers.put(type, (message, connection) -> executor.execute(() -> handler.accept(message.sender(), message.payload())));
    }

    @Override
    public void registerHandler(String type, BiFunction<Endpoint, byte[], byte[]> handler, Executor executor) {
        checkPermission(CLUSTER_WRITE);
        handlers.put(type, (message, connection) -> executor.execute(() -> {
            byte[] responsePayload = null;
            Status status = Status.OK;
            try {
                responsePayload = handler.apply(message.sender(), message.payload());
            } catch (Exception e) {
                status = Status.ERROR_HANDLER_EXCEPTION;
            }
            connection.reply(message, status, Optional.ofNullable(responsePayload));
        }));
    }

    @Override
    public void registerHandler(String type, BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> handler) {
        checkPermission(CLUSTER_WRITE);
        handlers.put(type, (message, connection) -> {
            handler.apply(message.sender(), message.payload()).whenComplete((result, error) -> {
                Status status = error == null ? Status.OK : Status.ERROR_HANDLER_EXCEPTION;
                connection.reply(message, status, Optional.ofNullable(result));
            });
        });
    }

    @Override
    public void unregisterHandler(String type) {
        checkPermission(CLUSTER_WRITE);
        handlers.remove(type);
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
            b.childHandler(new OnosCommunicationChannelInitializer());
        }
        b.option(ChannelOption.SO_BACKLOG, 128);
        b.childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.
        b.bind(localEp.port()).sync().addListener(future -> {
            if (future.isSuccess()) {
                log.info("{} accepting incoming connections on port {}", localEp.host(), localEp.port());
            } else {
                log.warn("{} failed to bind to port {} due to {}", localEp.host(), localEp.port(), future.cause());
            }
        });
    }

    private class OnosCommunicationChannelFactory
            implements KeyedPoolableObjectFactory<Endpoint, Connection> {

        @Override
        public void activateObject(Endpoint endpoint, Connection connection)
                throws Exception {
        }

        @Override
        public void destroyObject(Endpoint ep, Connection connection) throws Exception {
            log.debug("Closing connection {} to {}", connection, ep);
            //Is this the right way to destroy?
            connection.destroy();
        }

        @Override
        public Connection makeObject(Endpoint ep) throws Exception {
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
            if (enableNettyTls) {
                bootstrap.handler(new SslClientCommunicationChannelInitializer());
            } else {
                bootstrap.handler(new OnosCommunicationChannelInitializer());
            }
            // Start the client.
            CompletableFuture<Channel> retFuture = new CompletableFuture<>();
            ChannelFuture f = bootstrap.connect(ep.host().toInetAddress(), ep.port());

            f.addListener(future -> {
                if (future.isSuccess()) {
                    retFuture.complete(f.channel());
                } else {
                    retFuture.completeExceptionally(future.cause());
                }
            });
            log.debug("Established a new connection to {}", ep);
            return new Connection(retFuture);
        }

        @Override
        public void passivateObject(Endpoint ep, Connection connection)
                throws Exception {
        }

        @Override
        public boolean validateObject(Endpoint ep, Connection connection) {
            return connection.validate();
        }
    }

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

    private class OnosCommunicationChannelInitializer extends ChannelInitializer<SocketChannel> {

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

    @ChannelHandler.Sharable
    private class InboundMessageDispatcher extends SimpleChannelInboundHandler<Object> {
     // Effectively SimpleChannelInboundHandler<InternalMessage>,
     // had to specify <Object> to avoid Class Loader not being able to find some classes.

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object rawMessage) throws Exception {
            InternalMessage message = (InternalMessage) rawMessage;
            try {
                dispatchLocally(message, connections.get(ctx.channel()));
            } catch (RejectedExecutionException e) {
                log.warn("Unable to dispatch message due to {}", e.getMessage());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            log.error("Exception inside channel handling pipeline.", cause);
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

    private void dispatchLocally(InternalMessage message, Connection connection) throws IOException {
        if (message.preamble() != preamble) {
            log.debug("Received {} with invalid preamble from {}", message.type(), message.sender());
            connection.reply(message, Status.PROTOCOL_EXCEPTION, Optional.empty());
            return;
        }

        clockService.recordEventTime(message.time());
        String type = message.type();
        if (REPLY_MESSAGE_TYPE.equals(type)) {
            Callback callback = connection.callbacks.remove(message.id());
            if (callback != null) {
                if (message.status() == Status.OK) {
                    callback.complete(message.payload());
                } else if (message.status() == Status.ERROR_NO_HANDLER) {
                    callback.completeExceptionally(new MessagingException.NoRemoteHandler());
                } else if (message.status() == Status.ERROR_HANDLER_EXCEPTION) {
                    callback.completeExceptionally(new MessagingException.RemoteHandlerFailure());
                } else if (message.status() == Status.PROTOCOL_EXCEPTION) {
                    callback.completeExceptionally(new MessagingException.ProtocolException());
                }
                connection.samples.addValue(System.currentTimeMillis() - callback.time);
            } else {
                log.debug("Received a reply for message id:[{}]. "
                        + " from {}. But was unable to locate the"
                        + " request handle", message.id(), message.sender());
            }
            return;
        }

        BiConsumer<InternalMessage, Connection> handler = handlers.get(type);
        if (handler != null) {
            handler.accept(message, connection);
        } else {
            log.debug("No handler for message type {} from {}", message.type(), message.sender());
            connection.reply(message, Status.ERROR_NO_HANDLER, Optional.empty());
        }
    }

    private final class Callback {
        private final CompletableFuture<byte[]> future;
        private final Executor executor;
        private final long time = System.currentTimeMillis();

        public Callback(CompletableFuture<byte[]> future, Executor executor) {
            this.future = future;
            this.executor = executor;
        }

        public void complete(byte[] value) {
            executor.execute(() -> future.complete(value));
        }

        public void completeExceptionally(Throwable error) {
            executor.execute(() -> future.completeExceptionally(error));
        }
    }

    private final class Connection {
        private final DescriptiveStatistics samples = new SynchronizedDescriptiveStatistics(WINDOW_SIZE);
        private final CompletableFuture<Channel> internalFuture;
        private final Map<Long, Callback> callbacks = Maps.newConcurrentMap();
        private ScheduledFuture<?> cleanupFuture;

        public Connection(CompletableFuture<Channel> internalFuture) {
            this.internalFuture = internalFuture;
            internalFuture.thenAccept(channel -> connections.put(channel, this));
            cleanupFuture = serverGroup.schedule(this::cleanup, DEFAULT_TIMEOUT_MILLIS / 2, TimeUnit.MILLISECONDS);
        }

        /**
         * Cleans up callbacks.
         */
        private void cleanup() {
            long currentTime = System.currentTimeMillis();
            long currentTimeout = computeTimeoutMillis();
            Iterator<Map.Entry<Long, Callback>> iterator = callbacks.entrySet().iterator();
            while (iterator.hasNext()) {
                Callback callback = iterator.next().getValue();
                if (currentTime - callback.time > currentTimeout) {
                    iterator.remove();
                    callback.completeExceptionally(new TimeoutException("Request timed out in " + (currentTime - callback.time) + " milliseconds"));
                }
            }
            cleanupFuture = serverGroup.schedule(this::cleanup, currentTimeout / 2, TimeUnit.MILLISECONDS);
        }

        /**
         * Computes the current timeout.
         *
         * @return the current timeout
         */
        private long computeTimeoutMillis() {
            if (samples.getN() < MIN_SAMPLES) {
                return DEFAULT_TIMEOUT_MILLIS;
            }
            long timeout = (long) (samples.getPercentile(TIMEOUT_PERCENTILE) * TIMEOUT_MULTIPLIER);
            return Math.max(MIN_TIMEOUT_MILLIS, timeout);
        }

        /**
         * Sends a message out on its channel and associated the message with a
         * completable future used for signaling.
         * @param message the message to be sent
         * @param callback a callback to call with the response or {@code null} if no response is expected
         * @return a future to be completed once the message has been sent
         */
        public CompletableFuture<Void> send(InternalMessage message, Callback callback) {
            callbacks.put(message.id(), callback);
            CompletableFuture<Void> future = new CompletableFuture<>();
            internalFuture.whenComplete((channel, throwable) -> {
                if (throwable == null) {
                    channel.writeAndFlush(message).addListener(channelFuture -> {
                        if (!channelFuture.isSuccess()) {
                            callbacks.remove(message.id());
                            future.completeExceptionally(channelFuture.cause());
                        } else {
                            future.complete(null);
                        }
                    });
                } else {
                    callbacks.remove(message.id());
                    future.completeExceptionally(throwable);
                }
            });
            return future;
        }

        /**
         * Replies to a message.
         */
        public void reply(InternalMessage message, Status status, Optional<byte[]> responsePayload) {
            InternalMessage response = new InternalMessage(preamble,
                    clockService.timeNow(),
                    message.id(),
                    localEp,
                    REPLY_MESSAGE_TYPE,
                    responsePayload.orElse(new byte[0]),
                    status);
            internalFuture.thenAccept(channel -> channel.writeAndFlush(response));
        }

        /**
         * Destroys a channel by closing its channel (if it exists) and
         * cancelling its future.
         */
        public void destroy() {
            Channel channel = internalFuture.getNow(null);
            if (channel != null) {
                channel.close();
                connections.remove(channel);
            }
            ScheduledFuture<?> cleanupFuture = this.cleanupFuture;
            if (cleanupFuture != null) {
                cleanupFuture.cancel(false);
            }
            internalFuture.cancel(false);
        }

        /**
         * Determines whether the connection is valid meaning it is either
         * complete with and active channel
         * or it has not yet completed.
         * @return true if the channel has an active connection or has not
         * yet completed
         */
        public boolean validate() {
            if (internalFuture.isCompletedExceptionally()) {
                return false;
            }
            Channel channel = internalFuture.getNow(null);
            return channel == null || channel.isActive();
        }
    }
}
