package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisPipeline;
import cc.whohow.redis.codec.Codecs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.redisson.api.RFuture;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommands;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * 有序集合
 */
public class RedisSortedSet<E> implements ConcurrentMap<E, Number> {
    protected final Redis redis;
    protected final ByteBuf name;
    protected final Codec codec;

    public RedisSortedSet(Redis redis, String name, Codec codec) {
        this.redis = redis;
        this.name = Unpooled.copiedBuffer(name, StandardCharsets.UTF_8).asReadOnly();
        this.codec = codec;
    }

    @Override
    public int size() {
        return redis.execute(RedisCommands.ZCARD_INT, name.retain());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return redis.execute(RedisCommands.ZCOUNT, name.retain(), value, value) > 0;
    }

    @Override
    public Number get(Object key) {
        return redis.execute(RedisCommands.ZSCORE, name.retain(), Codecs.encode(codec, key));
    }

    @Override
    public Number put(E key, Number value) {
        ByteBuf encodedKey = Codecs.encode(codec, key);
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<Double> r = pipeline.execute(RedisCommands.ZSCORE, name.retain(), encodedKey.retain());
        pipeline.execute(RedisCommands.ZADD, name.retain(), value, encodedKey);
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public long zadd(E key, Number value) {
        return redis.execute(RedisCommands.ZADD, name.retain(), value, Codecs.encode(codec, key));
    }

    @Override
    public Number remove(Object key) {
        ByteBuf encodedKey = Codecs.encode(codec, key);
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<Double> r = pipeline.execute(RedisCommands.ZSCORE, name.retain(), encodedKey.retain());
        pipeline.execute(RedisCommands.ZREM, name.retain(), encodedKey);
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public boolean zrem(E key) {
        return redis.execute(RedisCommands.ZREM, name.retain(), Codecs.encode(codec, key));
    }

    @Override
    public void putAll(Map<? extends E, ? extends Number> m) {
        ByteBuf[] encodedKeys = Codecs.encode(codec, m.keySet());
        Object[] params = new Object[1 + m.size() * 2];
        params[0] = name.retain();
        int i = 0;
        for (Number value : m.values()) {
            params[i * 2 + 1] = value;
            params[i * 2 + 2] = encodedKeys[i];
            i++;
        }
        redis.execute(RedisCommands.ZADD, params);
    }

    @Override
    public void clear() {
        redis.execute(RedisCommands.DEL, name.retain());
    }

    @Override
    @Deprecated
    public Set<E> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Collection<Number> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Set<Entry<E, Number>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Number getOrDefault(Object key, Number defaultValue) {
        Number value = get(key);
        return value == null ? defaultValue : value;
    }

    @Override
    public Number putIfAbsent(E key, Number value) {
        ByteBuf encodedKey = Codecs.encode(codec, key);
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<Double> r = pipeline.execute(RedisCommands.ZSCORE, name.retain(), encodedKey.retain());
        pipeline.execute(RedisCommands.ZADD, name.retain(), "NX", value, Codecs.encode(codec, key));
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public long zaddnx(E key, Number value) {
        return redis.execute(RedisCommands.ZADD, name.retain(), "NX", value, Codecs.encode(codec, key));
    }

    @Override
    @Deprecated
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean replace(E key, Number oldValue, Number newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Number replace(E key, Number value) {
        ByteBuf encodedKey = Codecs.encode(codec, key);
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<Double> r = pipeline.execute(RedisCommands.ZSCORE, name.retain(), encodedKey.retain());
        pipeline.execute(RedisCommands.ZADD, name.retain(), "XX", value, Codecs.encode(codec, key));
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public long zaddxx(E key, Number value) {
        return redis.execute(RedisCommands.ZADD, name.retain(), "XX", value, Codecs.encode(codec, key));
    }

    @Override
    public String toString() {
        return "RedisSortedSet{" +
                "redis=" + redis +
                ", name=" + name.toString(StandardCharsets.UTF_8) +
                ", codec=" + codec +
                '}';
    }
}
