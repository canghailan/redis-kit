package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisPipeline;
import cc.whohow.redis.codec.Codecs;
import cc.whohow.redis.codec.OptionalCodec;
import io.netty.buffer.ByteBuf;
import org.redisson.api.RFuture;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommands;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 键集合
 */
public class RedisKey<V> implements ConcurrentMap<String, V> {
    protected final Redis redis;
    protected final Codec codec;
    protected final Codec optionalCodec;

    public RedisKey(Redis redis, Codec codec) {
        this.redis = redis;
        this.codec = codec;
        this.optionalCodec = new OptionalCodec(codec);
    }

    @Override
    public int size() {
        return redis.execute(RedisCommands.DBSIZE).intValue();
    }

    @Override
    public boolean isEmpty() {
        return redis.execute(RedisCommands.RANDOM_KEY) != null;
    }

    @Override
    public boolean containsKey(Object key) {
        return redis.execute(RedisCommands.EXISTS, key);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        return redis.execute(codec, RedisCommands.GET, key);
    }

    public Optional<V> getOptional(Object key) {
        return redis.execute(optionalCodec, RedisCommands.GET, key);
    }

    @Override
    public V put(String key, V value) {
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<V> r = pipeline.execute(codec, RedisCommands.GET, key);
        pipeline.execute(RedisCommands.SET, key, Codecs.encode(codec, value));
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public void set(String key, V value) {
        redis.execute(RedisCommands.SET, key, Codecs.encode(codec, value));
    }

    @Override
    public V remove(Object key) {
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<V> r = pipeline.execute(codec, RedisCommands.GET, key);
        pipeline.execute(RedisCommands.DEL, key);
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public long del(Object key) {
        return redis.execute(RedisCommands.DEL, key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        ByteBuf[] encodedValues = Codecs.encode(codec, m.values());
        Object[] params = new Object[m.size() * 2];
        int i = 0;
        for (String key : m.keySet()) {
            params[2 * i] = key;
            params[2 * i + 1] = encodedValues[i];
            i++;
        }
        redis.execute(RedisCommands.MSET, params);
    }

    @Override
    public void clear() {
        redis.execute(RedisCommands.FLUSHDB);
    }

    @Override
    @Deprecated
    public Set<String> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Set<Entry<String, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        Optional<V> value = getOptional(key);
        return value == null ? defaultValue : value.orElse(null);
    }

    @Override
    public V putIfAbsent(String key, V value) {
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<V> r = pipeline.execute(codec, RedisCommands.GET, key);
        pipeline.execute(RedisCommands.SET, key, Codecs.encode(codec, value), "NX");
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public void setnx(String key, V value) {
        redis.execute(RedisCommands.SET, key, Codecs.encode(codec, value), "NX");
    }

    public void setpx(String key, V value, long millis) {
        redis.execute(RedisCommands.SET, key, Codecs.encode(codec, value), "PX", millis);
    }

    @Override
    @Deprecated
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean replace(String key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V replace(String key, V value) {
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<V> r = pipeline.execute(codec, RedisCommands.GET, key);
        pipeline.execute(RedisCommands.SET, key, Codecs.encode(codec, value), "XX");
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public void setxx(String key, V value) {
        redis.execute(RedisCommands.SET, key, Codecs.encode(codec, value), "XX");
    }

    @Override
    @Deprecated
    public void replaceAll(BiFunction<? super String, ? super V, ? extends V> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V computeIfAbsent(String key, Function<? super String, ? extends V> mappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V computeIfPresent(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V compute(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V merge(String key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "RedisKey{" +
                "redis=" + redis +
                ", codec=" + codec +
                '}';
    }
}
