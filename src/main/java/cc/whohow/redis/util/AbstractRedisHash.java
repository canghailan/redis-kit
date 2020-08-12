package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AbstractRedisHash<K, V> {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<K> keyCodec;
    protected final Codec<V> valueCodec;
    protected final ByteBuffer hashKey;

    public AbstractRedisHash(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<K> keyCodec, Codec<V> valueCodec, ByteBuffer key) {
        this.redis = redis;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.hashKey = key;
    }

    public ByteBuffer encodeKey(K key) {
        return keyCodec.encode(key);
    }

    public K decodeKey(ByteBuffer buffer) {
        return keyCodec.decode(buffer);
    }

    public ByteBuffer encodeValue(V value) {
        return valueCodec.encode(value);
    }

    public V decodeValue(ByteBuffer buffer) {
        return valueCodec.decode(buffer);
    }

    public KeyValue<K, V> decode(KeyValue<ByteBuffer, ByteBuffer> keyValue) {
        return KeyValue.fromNullable(decodeKey(keyValue.getKey()), decodeValue(keyValue.getValueOrElse(null)));
    }

    public Long hlen() {
        if (log.isTraceEnabled()) {
            log.trace("HLEN {}", toString());
        }
        return redis.hlen(hashKey.duplicate());
    }

    public boolean hexists(K key) {
        if (log.isTraceEnabled()) {
            log.trace("HEXISTS {} {}", toString(), key);
        }
        return redis.hexists(hashKey.duplicate(), encodeKey(key));
    }

    public V hget(K key) {
        if (log.isTraceEnabled()) {
            log.trace("HGET {} {}", toString(), key);
        }
        return decodeValue(redis.hget(hashKey.duplicate(), encodeKey(key)));
    }

    public Stream<KeyValue<K, V>> hmget(Collection<? extends K> keys) {
        if (log.isTraceEnabled()) {
            log.trace("HMGET {} {}", toString(), keys);
        }
        return redis.hmget(hashKey.duplicate(), keys.stream()
                .map(this::encodeKey)
                .peek(ByteBuffer::mark) // HGET复用key对象问题
                .toArray(ByteBuffer[]::new)).stream()
                .peek(kv -> kv.getKey().reset()) // HGET复用key对象问题
                .map(this::decode);
    }

    public Map<K, V> hgetall() {
        if (log.isTraceEnabled()) {
            log.trace("HGETALL {}", toString());
        }
        return redis.hgetall(hashKey.duplicate()).entrySet().stream()
                .collect(Collectors.toMap(
                        e -> decodeKey(e.getKey()),
                        e -> decodeValue(e.getValue()),
                        (a, b) -> b,
                        LinkedHashMap::new));
    }

    public boolean hset(K key, V value) {
        if (log.isTraceEnabled()) {
            log.trace("HSET {} {} {}", toString(), key, value);
        }
        return redis.hset(hashKey.duplicate(), encodeKey(key), encodeValue(value));
    }

    public boolean hsetnx(K key, V value) {
        if (log.isTraceEnabled()) {
            log.trace("HSETNX {} {} {}", toString(), key, value);
        }
        return redis.hsetnx(hashKey.duplicate(), encodeKey(key), encodeValue(value));
    }

    public void hmset(Map<? extends K, ? extends V> m) {
        if (log.isTraceEnabled()) {
            log.trace("HMSET {} {}", toString(), m);
        }
        redis.hmset(hashKey.duplicate(), m.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> encodeKey(e.getKey()),
                        e -> encodeValue(e.getValue()))));
    }

    public Long hdel(K key) {
        if (log.isTraceEnabled()) {
            log.trace("HDEL {} {}", toString(), key);
        }
        return redis.hdel(hashKey.duplicate(), encodeKey(key));
    }

    public Long del() {
        if (log.isTraceEnabled()) {
            log.trace("DEL {}", toString());
        }
        return redis.del(hashKey.duplicate());
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(hashKey);
    }
}
