package cc.whohow.redis.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisClientConfig;
import org.redisson.client.handler.RedisChannelInitializer;

import java.util.concurrent.atomic.LongAdder;

public class RedisChannelPoolInitializer extends RedisChannelInitializer implements ChannelPoolHandler {
    private final LongAdder releasedCounter = new LongAdder();
    private final LongAdder acquiredCounter = new LongAdder();
    private final LongAdder createdCounter = new LongAdder();

    public RedisChannelPoolInitializer(Bootstrap bootstrap, RedisClientConfig config, RedisClient redisClient, ChannelGroup channels, Type type) {
        super(bootstrap, config, redisClient, channels, type);
    }

    @Override
    public void channelReleased(Channel ch) {
        releasedCounter.increment();
    }

    @Override
    public void channelAcquired(Channel ch) {
        acquiredCounter.increment();
    }

    @Override
    public void channelCreated(Channel ch) throws Exception {
        initChannel(ch);
        createdCounter.increment();
    }

    public long getReleasedCount() {
        return releasedCounter.longValue();
    }

    public long getAcquiredCount() {
        return acquiredCounter.longValue();
    }

    public long getCreatedCount() {
        return createdCounter.longValue();
    }

    public long getInUsingCount() {
        return acquiredCounter.longValue() - releasedCounter.longValue();
    }

    @Override
    public String toString() {
        return "RedisChannelPool{" +
                "releasedCount=" + getReleasedCount() +
                ", acquiredCount=" + getAcquiredCount() +
                ", createdCount=" + getCreatedCount() +
                ", inUsingCount=" + getInUsingCount() +
                '}';
    }
}
