package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;

public class RedisTimeSequence {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final String id;
    protected final ByteBuffer encodedId;

    public RedisTimeSequence(RedisCommands<ByteBuffer, ByteBuffer> redis, String id) {
        this.redis = redis;
        this.id = id;
        this.encodedId = ByteBuffers.fromUtf8(id);
    }

    public Instant time() {
        List<ByteBuffer> time = redis.time();
        ByteBuffer seconds = time.get(0);
        ByteBuffer microseconds = time.get(1);
        return Instant.ofEpochSecond(
                PrimitiveCodec.LONG.decode(seconds),
                PrimitiveCodec.INTEGER.decode(microseconds) * 1000);
    }

    public long sequence(Instant time) {
        return redis.hincrby(encodedId.duplicate(), PrimitiveCodec.LONG.encode(time.getEpochSecond()), 1);
    }

    public void gc() {
        redis.hscan(encodedId.duplicate());
    }
}
