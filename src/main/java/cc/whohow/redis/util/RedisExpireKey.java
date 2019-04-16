package cc.whohow.redis.util;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.lettuce.Lettuce;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

/**
 * 键集合
 */
public class RedisExpireKey<V> extends RedisKey<V> {
    protected final long ttl;
    protected final SetArgs px;
    protected final SetArgs pxNx;
    protected final SetArgs pxXx;

    public RedisExpireKey(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec, Duration ttl) {
        super(redis, codec);
        this.ttl = ttl.toMillis();
        this.px = SetArgs.Builder.px(this.ttl);
        this.pxNx = SetArgs.Builder.px(this.ttl).nx();
        this.pxXx = SetArgs.Builder.px(this.ttl).xx();
    }

    public RedisExpireKey(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec, Codec<String> keyCodec, Duration ttl) {
        super(redis, codec, keyCodec);
        this.ttl = ttl.toMillis();
        this.px = SetArgs.Builder.px(this.ttl);
        this.pxNx = SetArgs.Builder.px(this.ttl).nx();
        this.pxXx = SetArgs.Builder.px(this.ttl).xx();
    }

    /**
     * @return value
     */
    @Override
    public V put(String key, V value) {
        set(key, value);
        return value;
    }

    public void set(String key, V value) {
        redis.set(encodeKey(key), encode(value), px);
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return null/value
     */
    @Override
    public V putIfAbsent(String key, V value) {
        if (Lettuce.ok(redis.set(encodeKey(key), encode(value), pxNx))) {
            return null;
        }
        return value;
    }

    /**
     * @return null/value
     */
    @Override
    public V replace(String key, V value) {
        if (Lettuce.ok(redis.set(encodeKey(key), encode(value), pxXx))) {
            return null;
        }
        return value;
    }
}
