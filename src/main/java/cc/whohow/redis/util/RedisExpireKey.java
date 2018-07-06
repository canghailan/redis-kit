package cc.whohow.redis.util;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.lettuce.Lettuce;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

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

    /**
     * @return value
     */
    @Override
    public V put(String key, V value) {
        set(key, value);
        return value;
    }

    public void set(String key, V value) {
        redis.set(encodeKey(key), encodeValue(value), px);
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        Map<ByteBuffer, ByteBuffer> encodedKeyValues = m.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> encodeKey(e.getKey()),
                        e -> encodeValue(e.getValue())));
        redis.mset(encodedKeyValues);
    }

    /**
     * @return null/value
     */
    @Override
    public V putIfAbsent(String key, V value) {
        if (Lettuce.ok(redis.set(encodeKey(key), encodeValue(value), pxNx))) {
            return null;
        }
        return value;
    }

    /**
     * @return null/value
     */
    @Override
    public V replace(String key, V value) {
        if (Lettuce.ok(redis.set(encodeKey(key), encodeValue(value), pxXx))) {
            return null;
        }
        return value;
    }
}
