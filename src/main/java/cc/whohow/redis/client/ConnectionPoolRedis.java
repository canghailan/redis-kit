package cc.whohow.redis.client;

import cc.whohow.redis.Redis;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.client.*;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.RedisChannelInitializer;
import org.redisson.client.protocol.CommandData;
import org.redisson.client.protocol.CommandsData;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于Netty连接池的Redis连接池
 *
 * @see RedisClient
 * @see ChannelPool
 */
public class ConnectionPoolRedis implements Redis {
    private static final Logger log = LogManager.getLogger();

    protected final Config config;
    protected final RedisClient redisClient;
    protected final Bootstrap bootstrap;
    protected final ChannelPool channelPool;

    public ConnectionPoolRedis(Config config) {
        this.config = config;
        this.redisClient = RedisClient.create(newRedisClientConfig(config));
        this.bootstrap = unsafeGet(redisClient, "bootstrap");
        this.channelPool = new FixedChannelPool(
                bootstrap,
                new RedisChannelPoolInitializer(
                        bootstrap,
                        redisClient.getConfig(),
                        redisClient,
                        unsafeGet(redisClient, "channels"),
                        RedisChannelInitializer.Type.PLAIN),
                config.useSingleServer().getConnectionPoolSize());
    }

    @SuppressWarnings("unchecked")
    protected static <T> T unsafeGet(Object object, String name) {
        try {
            Field field = object.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return (T) field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    protected RedisClientConfig newRedisClientConfig(Config config) {
        SingleServerConfig singleServerConfig = config.useSingleServer();
        RedisClientConfig redisConfig = new RedisClientConfig();
        redisConfig.setAddress(singleServerConfig.getAddress())
                .setExecutor(config.getExecutor())
                .setGroup(config.getEventLoopGroup())
                .setConnectTimeout(singleServerConfig.getConnectTimeout())
                .setCommandTimeout(singleServerConfig.getTimeout())
                .setSslEnableEndpointIdentification(singleServerConfig.isSslEnableEndpointIdentification())
                .setSslProvider(singleServerConfig.getSslProvider())
                .setSslTruststore(singleServerConfig.getSslTruststore())
                .setSslTruststorePassword(singleServerConfig.getSslTruststorePassword())
                .setSslKeystore(singleServerConfig.getSslKeystore())
                .setSslKeystorePassword(singleServerConfig.getSslKeystorePassword())
                .setClientName(singleServerConfig.getClientName())
                .setKeepPubSubOrder(config.isKeepPubSubOrder())
                .setPingConnectionInterval(singleServerConfig.getPingConnectionInterval())
                .setKeepAlive(singleServerConfig.isKeepAlive())
                .setTcpNoDelay(singleServerConfig.isTcpNoDelay())
                .setDatabase(singleServerConfig.getDatabase())
                .setPassword(singleServerConfig.getPassword());
        return redisConfig;
    }

    @Override
    public URI getUri() {
        SingleServerConfig singleServerConfig = config.useSingleServer();
        return singleServerConfig.getAddress().resolve(String.valueOf(singleServerConfig.getDatabase()));
    }

    @Override
    public <T> T execute(RedisCommand<T> command, Object... params) {
        return execute(null, command, params);
    }

    @Override
    public <T, R> R execute(Codec codec, RedisCommand<T> command, Object... params) {
        log.trace("{}", command);
        // TODO optimize
        Future<Channel> channelFuture = channelPool.acquire().syncUninterruptibly();
        if (channelFuture.isSuccess()) {
            Channel channel = channelFuture.getNow();
            try {
                RedisConnection connection = RedisConnection.getFrom(channel);
                CommandData<T, R> commandData = new CommandData<>(new RedissonPromise<>(), codec, command, params);
                connection.send(commandData);
                return commandData.getPromise().syncUninterruptibly().getNow();
            } finally {
                channelPool.release(channel);
            }
        } else {
            throw new RedisException("acquire connection error", channelFuture.cause());
        }
    }

    @Override
    public RedisPipeline pipeline() {
        return new Pipeline(this);
    }

    @Override
    public RedisPooledConnection getConnection() {
        Future<Channel> channelFuture = channelPool.acquire().syncUninterruptibly();
        if (channelFuture.isSuccess()) {
            Channel channel = channelFuture.getNow();
            try {
                return new Connection(channelPool, RedisConnection.getFrom(channel));
            } catch (RuntimeException e) {
                channelPool.release(channel);
                throw e;
            }
        } else {
            throw new RedisException("acquire connection error", channelFuture.cause());
        }
    }

    @Override
    public RedisPubSubConnection getPubSubConnection() {
        return redisClient.connectPubSub();
    }

    @Override
    public void close() {
        try {
            channelPool.close();
        } finally {
            redisClient.shutdown();
        }
    }

    static class Connection implements RedisPooledConnection {
        private ChannelPool channelPool;
        private RedisConnection connection;

        public Connection(ChannelPool channelPool, RedisConnection connection) {
            this.channelPool = channelPool;
            this.connection = connection;
        }

        @Override
        public RedisConnection getConnection() {
            return connection;
        }

        @Override
        public void close() {
            channelPool.release(connection.getChannel());
        }
    }

    static class Pipeline implements RedisPipeline {
        private final ConnectionPoolRedis redis;
        private final List<CommandData<?, ?>> commands = new ArrayList<>();

        public Pipeline(ConnectionPoolRedis redis) {
            this.redis = redis;
        }

        @Override
        public <T> RPromise<T> execute(RedisCommand<T> command, Object... params) {
            return execute(null, command, params);
        }

        @Override
        public <T, R> RPromise<R> execute(Codec codec, RedisCommand<T> command, Object... params) {
            log.trace("pipeline {}", command);
            CommandData<T, R> commandData = new CommandData<>(new RedissonPromise<>(), codec, command, params);
            commands.add(commandData);
            return commandData.getPromise();
        }

        @Override
        public void sync() {
            log.trace("pipeline ->");
            Future<Channel> channelFuture = redis.channelPool.acquire().syncUninterruptibly();
            if (channelFuture.isSuccess()) {
                Channel channel = channelFuture.getNow();
                try {
                    RedisConnection connection = RedisConnection.getFrom(channel);
                    CommandsData commandsData = new CommandsData(new RedissonPromise<>(), commands);
                    connection.send(commandsData);
                    commandsData.getPromise().syncUninterruptibly();
                } finally {
                    redis.channelPool.release(channel);
                }
            } else {
                throw new RedisException("acquire connection error", channelFuture.cause());
            }
        }
    }
}
