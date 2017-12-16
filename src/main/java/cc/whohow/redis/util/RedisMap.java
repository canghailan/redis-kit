package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisPipeline;
import cc.whohow.redis.codec.Codecs;
import cc.whohow.redis.codec.OptionalCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.redisson.api.RFuture;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommands;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 散列表
 */
public class RedisMap<K, V> implements ConcurrentMap<K, V> {
    protected final Redis redis;
    protected final ByteBuf name;
    protected final Codec codec;
    protected final Codec optionalCodec;

    public RedisMap(Redis redis, String name, Codec codec) {
        this.redis = redis;
        this.name = Unpooled.copiedBuffer(name, StandardCharsets.UTF_8).asReadOnly();
        this.codec = codec;
        this.optionalCodec = new OptionalCodec(codec);
    }

    @Override
    public int size() {
        return redis.execute(RedisCommands.HLEN, name);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return redis.execute(RedisCommands.HEXISTS, name, Codecs.encodeMapKey(codec, key));
    }

    @Override
    @Deprecated
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        return redis.execute(codec, RedisCommands.HGET, name, Codecs.encodeMapKey(codec, key));
    }

    @Override
    public V put(K key, V value) {
        ByteBuf encodedKey = Codecs.encodeMapKey(codec, key);
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<V> r = pipeline.execute(codec, RedisCommands.HGET, name, encodedKey.retain());
        pipeline.execute(RedisCommands.HSET, name, encodedKey, Codecs.encodeMapValue(codec, value));
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public boolean hset(K key, V value) {
        return redis.execute(RedisCommands.HSET, name, Codecs.encodeMapKey(codec, key), Codecs.encodeMapValue(codec, value));
    }

    @Override
    public V remove(Object key) {
        ByteBuf encodedKey = Codecs.encodeMapKey(codec, key);
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<V> r = pipeline.execute(codec, RedisCommands.HGET, name, encodedKey.retain());
        pipeline.execute(RedisCommands.HDEL, name, encodedKey);
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public boolean hdel(K key) {
        return redis.execute(RedisCommands.HDEL, name, Codecs.encodeMapKey(codec, key)) > 0;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        redis.execute(RedisCommands.HMSET, (Object[]) Codecs.concat(name, Codecs.encodeMapKeyValue(codec, m)));
    }

    @Override
    public void clear() {
        redis.execute(RedisCommands.DEL, name);
    }

    @Override
    @Deprecated
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        Optional<V> optionalValue = redis.execute(optionalCodec, RedisCommands.HGET, name, Codecs.encodeMapKey(codec, key));
        return optionalValue == null ? defaultValue : optionalValue.orElse(null);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        ByteBuf encodedKey = Codecs.encodeMapKey(codec, key);
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<V> r = pipeline.execute(codec, RedisCommands.HGET, name, encodedKey.retain());
        pipeline.execute(RedisCommands.HSETNX, name, encodedKey, Codecs.encodeMapValue(codec, value));
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public boolean hsetnx(K key, V value) {
        return redis.execute(RedisCommands.HSETNX, name, Codecs.encodeMapKey(codec, key), Codecs.encodeMapValue(codec, value));
    }

    @Override
    @Deprecated
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V replace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "RedisMap{" +
                "redis=" + redis +
                ", name=" + name.toString(StandardCharsets.UTF_8) +
                ", codec=" + codec +
                '}';
    }
}
