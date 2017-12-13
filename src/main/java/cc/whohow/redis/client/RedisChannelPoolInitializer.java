package cc.whohow.redis.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisClientConfig;
import org.redisson.client.handler.RedisChannelInitializer;

public class RedisChannelPoolInitializer extends RedisChannelInitializer implements ChannelPoolHandler {
    public RedisChannelPoolInitializer(Bootstrap bootstrap, RedisClientConfig config, RedisClient redisClient, ChannelGroup channels, Type type) {
        super(bootstrap, config, redisClient, channels, type);
    }

    @Override
    public void channelReleased(Channel ch) {
    }

    @Override
    public void channelAcquired(Channel ch) {
    }

    @Override
    public void channelCreated(Channel ch) throws Exception {
        initChannel(ch);
    }
}
