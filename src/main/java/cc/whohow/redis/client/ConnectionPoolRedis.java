package cc.whohow.redis.client;

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
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.RedisChannelInitializer;
import org.redisson.client.protocol.CommandData;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.redisson.misc.RedissonPromise;

import java.lang.reflect.Field;

/**
 * @see RedisClient
 */
public class ConnectionPoolRedis implements Redis {
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
    public <T> T execute(RedisCommand<T> command, Object... params) {
        return execute(null, command, params);
    }

    @Override
    public <T, R> R execute(Codec codec, RedisCommand<T> command, Object... params) {
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
        }
        throw new RedisException("acquire connection error", channelFuture.cause());
    }

    @Override
    public void close() {
        try {
            channelPool.close();
        } finally {
            redisClient.shutdown();
        }
    }
}
