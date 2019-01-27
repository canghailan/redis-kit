package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Redis限流器
 */
public class RedisRateLimiter {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final RedisScriptCommands redisScriptCommands;
    protected final ByteBuffer key;
    protected final long maxPermits;
    protected final long perMilliseconds;

    public RedisRateLimiter(RedisCommands<ByteBuffer, ByteBuffer> redis, String key, long permits, Duration duration) {
        this(redis, ByteBuffers.fromUtf8(key), permits, duration);
    }

    public RedisRateLimiter(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, long permits, Duration duration) {
        this.redis = redis;
        this.redisScriptCommands = new RedisScriptCommands(redis);
        this.key = key;
        this.maxPermits = permits;
        this.perMilliseconds = duration.toMillis();
    }

    public double getRate() {
        return (double) maxPermits / (double) perMilliseconds;
    }

    public double acquire() {
        return acquire(1);
    }

    public double acquire(int permits) {
        if (tryAcquire(permits)) {
            return 0;
        }
        throw new UnsupportedOperationException();
    }

    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    public boolean tryAcquire(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException();
        }
        throw new UnsupportedOperationException();
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1, timeout, unit);
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }
}
