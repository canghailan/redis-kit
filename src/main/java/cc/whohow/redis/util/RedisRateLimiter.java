package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;

public class RedisRateLimiter {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final String id;
    protected final ByteBuffer encodedId;

    public RedisRateLimiter(RedisCommands<ByteBuffer, ByteBuffer> redis, String id) {
        this.redis = redis;
        this.id = id;
        this.encodedId = ByteBuffers.fromUtf8(id);
    }
}
