package cc.whohow.redis.pool;

import cc.whohow.redis.PooledRedisConnection;
import cc.whohow.redis.Redis;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.util.concurrent.Future;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisClientConfig;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisException;
import org.redisson.client.handler.RedisChannelInitializer;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.lang.reflect.Field;

/**
 * @see RedisClient
 */
public class RedisConnectionPool implements Redis {
    protected final Config config;
    protected final RedisClient redisClient;
    protected final Bootstrap bootstrap;
    protected final ChannelPool channelPool;

    public RedisConnectionPool(Config config) {
        this.config = config;
        this.redisClient = RedisClient.create(newRedisClientConfig(config));
        this.bootstrap = getFieldValue(redisClient, "bootstrap");
        this.channelPool = new FixedChannelPool(
                bootstrap,
                new PooledRedisChannelInitializer(
                        bootstrap,
                        redisClient.getConfig(),
                        redisClient,
                        getFieldValue(redisClient, "channels"),
                        RedisChannelInitializer.Type.PLAIN),
                config.useSingleServer().getConnectionPoolSize());
    }

    @SuppressWarnings("unchecked")
    protected static <T> T getFieldValue(Object object, String name) {
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
    public RedisConnection getConnection() {
        Future<Channel> channel = channelPool.acquire().syncUninterruptibly();
        if (channel.isSuccess()) {
            return RedisConnection.getFrom(channel.getNow());
        }
        throw new RedisException("acquire connection error", channel.cause());
    }

    @Override
    public PooledRedisConnection getPooledConnection() {
        return new Connection(this, getConnection());
    }

    public void release(RedisConnection connection) {
        channelPool.release(connection.getChannel());
    }

    @Override
    public void close() {
        try {
            channelPool.close();
        } finally {
            redisClient.shutdown();
        }
    }

    protected static class Connection implements PooledRedisConnection {
        protected final RedisConnectionPool connectionPool;
        protected final RedisConnection connection;

        public Connection(RedisConnectionPool connectionPool, RedisConnection connection) {
            this.connectionPool = connectionPool;
            this.connection = connection;
        }

        @Override
        public RedisConnection get() {
            return connection;
        }

        @Override
        public void close() {
            connectionPool.release(connection);
        }
    }
}
