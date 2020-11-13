package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.lettuce.*;
import io.lettuce.core.protocol.CommandType;

import java.util.*;

public class AbstractRedisHash<K, V> {
    protected final Redis redis;
    protected final Codec<K> keyCodec;
    protected final Codec<V> valueCodec;
    protected final ByteSequence hashKey;

    public AbstractRedisHash(Redis redis, Codec<K> keyCodec, Codec<V> valueCodec, ByteSequence key) {
        this.redis = redis;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.hashKey = key;
    }

    public Long hlen() {
        return redis.send(new IntegerOutput(), CommandType.HLEN, hashKey);
    }

    public Long hexists(K key) {
        return redis.send(new IntegerOutput(), CommandType.HEXISTS, hashKey, keyCodec.encode(key));
    }

    public V hget(K key) {
        return redis.send(new DecodeOutput<>(valueCodec::decode), CommandType.HGET, hashKey, keyCodec.encode(key));
    }

    public List<V> hmget(Collection<? extends K> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }
        List<ByteSequence> args = new ArrayList<>(1 + keys.size());
        args.add(hashKey);
        for (K key : keys) {
            args.add(keyCodec.encode(key));
        }
        return redis.send(new ListOutput<>(valueCodec::decode), CommandType.HMGET, args);
    }

    public Map<K, V> hgetall() {
        return redis.send(new MapOutput<>(keyCodec::decode, valueCodec::decode), CommandType.HGETALL, hashKey);
    }

    public Long hset(K key, V value) {
        return redis.send(new IntegerOutput(), CommandType.HSET, hashKey, keyCodec.encode(key), valueCodec.encode(value));
    }

    public Long hsetnx(K key, V value) {
        return redis.send(new IntegerOutput(), CommandType.HSETNX, hashKey, keyCodec.encode(key), valueCodec.encode(value));
    }

    public void hmset(Map<? extends K, ? extends V> m) {
        if (m.isEmpty()) {
            return;
        }
        List<ByteSequence> args = new ArrayList<>(1 + m.size() * 2);
        args.add(hashKey);
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            args.add(keyCodec.encode(e.getKey()));
            args.add(valueCodec.encode(e.getValue()));
        }
        redis.send(new VoidOutput(), CommandType.HMSET, args);
    }

    public Long hincrby(K key, long value) {
        return redis.send(new IntegerOutput(), CommandType.HINCRBY, hashKey, keyCodec.encode(key), RESP.b(value));
    }

    public RedisHashScanIterator<K, V> hscan() {
        return new RedisHashScanIterator<>(redis, keyCodec::decode, valueCodec::decode, hashKey);
    }

    public RedisHashScanIterator<K, V> hscan(ByteSequence pattern, int count) {
        return new RedisHashScanIterator<>(redis, keyCodec::decode, valueCodec::decode, hashKey, pattern, count);
    }

    public Long hdel(K key) {
        return redis.send(new IntegerOutput(), CommandType.HDEL, hashKey, keyCodec.encode(key));
    }

    public Long hdel(Collection<? extends K> keys) {
        if (keys.isEmpty()) {
            return 0L;
        }
        List<ByteSequence> args = new ArrayList<>(1 + keys.size());
        args.add(hashKey);
        for (K key : keys) {
            args.add(keyCodec.encode(key));
        }
        return redis.send(new IntegerOutput(), CommandType.HDEL, args);
    }

    public Long del() {
        return redis.send(new IntegerOutput(), CommandType.DEL, hashKey);
    }

    @Override
    public String toString() {
        return hashKey.toString();
    }
}
