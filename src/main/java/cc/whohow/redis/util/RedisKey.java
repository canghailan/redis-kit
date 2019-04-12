package cc.whohow.redis.util;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.StringCodec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 键集合
 */
public class RedisKey<V> implements ConcurrentMap<String, V> {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final RedisScriptCommands redisScriptCommands;
    protected final StringCodec keyCodec;
    protected final Codec<V> codec;

    public RedisKey(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec) {
        this(redis, codec, new StringCodec());
    }

    public RedisKey(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec, StringCodec keyCodec) {
        this.redis = redis;
        this.redisScriptCommands = new RedisScriptCommands(redis);
        this.codec = codec;
        this.keyCodec = keyCodec;
    }

    protected ByteBuffer encodeKey(String key) {
        return keyCodec.encode(key);
    }

    protected String decodeKey(ByteBuffer byteBuffer) {
        return keyCodec.decode(byteBuffer);
    }

    protected ByteBuffer encode(V value) {
        return codec.encode(value);
    }

    protected V decode(ByteBuffer byteBuffer) {
        return codec.decode(byteBuffer);
    }

    @Override
    public int size() {
        return redis.dbsize().intValue();
    }

    @Override
    public boolean isEmpty() {
        return redis.randomkey() != null;
    }

    @Override
    public boolean containsKey(Object key) {
        return redis.exists(encodeKey((String) key)) > 0;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        return decode(redis.get(encodeKey((String) key)));
    }

    @Override
    public V put(String key, V value) {
        return decode(redis.getset(encodeKey(key), encode(value)));
    }

    public void set(String key, V value) {
        redis.set(encodeKey(key), encode(value));
    }

    @Override
    public V remove(Object key) {
        return decode(redisScriptCommands.eval("getdel", ScriptOutputType.VALUE, encodeKey((String) key)));
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        redis.mset(m.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> encodeKey(e.getKey()),
                        e -> encode(e.getValue()))));
    }

    @Override
    public void clear() {
        redis.flushdb();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Set<Entry<String, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V putIfAbsent(String key, V value) {
        return decode(redisScriptCommands.eval("getset", ScriptOutputType.VALUE,
                new ByteBuffer[] {encodeKey(key)},
                new ByteBuffer[] {encode(value), Lettuce.nx()}));
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object key, Object value) {
        Long n = redisScriptCommands.eval("cad", ScriptOutputType.INTEGER,
                new ByteBuffer[] {encodeKey((String) key)},
                new ByteBuffer[] {encode((V) value)});
        return n > 0;
    }

    @Override
    public boolean replace(String key, V oldValue, V newValue) {
        return redisScriptCommands.eval("cas", ScriptOutputType.STATUS,
                new ByteBuffer[] {encodeKey(key)},
                new ByteBuffer[] {encode(oldValue), encode(newValue)});
    }

    @Override
    public V replace(String key, V value) {
        return decode(redisScriptCommands.eval("getset", ScriptOutputType.VALUE,
                new ByteBuffer[] {encodeKey(key)},
                new ByteBuffer[] {encode(value), Lettuce.xx()}));
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public void replaceAll(BiFunction<? super String, ? super V, ? extends V> function) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V computeIfAbsent(String key, Function<? super String, ? extends V> mappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V computeIfPresent(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V compute(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V merge(String key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }
}
