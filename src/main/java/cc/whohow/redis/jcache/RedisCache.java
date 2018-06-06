package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.codec.Codecs;
import cc.whohow.redis.codec.OptionalCodec;
import cc.whohow.redis.codec.OptionalRedisCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.jcache.processor.CacheMutableEntry;
import cc.whohow.redis.jcache.processor.EntryProcessorResultWrapper;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.ScanCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.decoder.ListScanResult;
import org.redisson.client.protocol.decoder.ScanObjectEntry;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 普通Redis缓存，不支持过期时间
 */
public class RedisCache<K, V> implements Cache<K, V> {
    public static final String OK = "OK";
    public static final SetArgs NX = SetArgs.Builder.nx();

    protected final RedisCacheManager cacheManager;
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final RedisCommands<ByteBuffer, ByteBuffer> commands;
    protected final OptionalRedisCodec<K, V> codec;

    public RedisCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration, Redis redis) {
        Objects.requireNonNull(configuration.getName());
        Objects.requireNonNull(configuration.getKeyCodec());
        Objects.requireNonNull(configuration.getValueCodec());
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        this.commands = null;
        this.codec = null;
    }

    @Override
    public V get(K key) {
        return codec.decodeValue(commands.get(codec.encodeKey(key)));
    }

    public Optional<V> getOptional(K key) {
        return codec.decodeOptionalValue(commands.get(codec.encodeKey(key)));
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> cacheLoader) {
        Optional<V> optional = getOptional(key);
        if (optional != null) {
            return optional.orElse(null);
        }
        V value = cacheLoader.apply(key);
        put(key, value);
        return value;
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        ByteBuffer[] encodedKeys = keys.stream()
                .map(key -> (K) key)
                .map(codec::encodeKey)
                .toArray(ByteBuffer[]::new);
        return commands.mget(encodedKeys).stream()
                .collect(Collectors.toMap(
                        kv -> codec.decodeKey(kv.getKey()),
                        kv -> codec.decodeValue(kv.getValueOrElse(null))));
    }

    @Override
    public boolean containsKey(K key) {
        return commands.exists(codec.encodeKey(key)) > 0;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) {
        commands.set(codec.encodeKey(key), codec.encodeValue(value));
    }

    @Override
    public V getAndPut(K key, V value) {
        return codec.decodeValue(commands.getset(codec.encodeKey(key), codec.encodeValue(value)));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        Map<ByteBuffer, ByteBuffer> encodedKeyValues = map.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> codec.encodeKey(e.getKey()),
                        e -> codec.encodeValue(e.getValue())));
        commands.mset(encodedKeyValues);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return OK.equals(commands.set(codec.encodeKey(key), codec.encodeValue(value), NX));
    }

    @Override
    public boolean remove(K key) {
        return redis.execute(RedisCommands.DEL, encodeRedisKey(key)) == 1L;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getAndRemove(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V value) {
        return redis.execute(RedisCommands.SETPXNX, encodeRedisKey(key), Codecs.encode(valueCodec, value), "XX");
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        redis.execute(RedisCommands.DEL, (Object[]) encodeRedisKeys(keys));
    }

    @Override
    public void removeAll() {
        Codec codec = new ScanCodec(StringCodec.INSTANCE);
        ByteBuf pattern = keyPrefix.copy().writeByte('*');
        Long pos = 0L;
        do {
            ListScanResult<ScanObjectEntry> result = redis.execute(
                    codec, RedisCommands.SCAN, pos, "MATCH", pattern, "COUNT", 100);
            if (!result.getValues().isEmpty()) {
                redis.execute(RedisCommands.DEL, result.getValues().stream().map(ScanObjectEntry::getBuf).toArray());
            }
            pos = result.getPos();
        } while (pos != 0);
    }

    @Override
    public void clear() {
        removeAll();
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(configuration)) {
            return clazz.cast(configuration);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return entryProcessor.process(new CacheMutableEntry<>(this, key), arguments);
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        Map<K, EntryProcessorResult<T>> results = new LinkedHashMap<>();
        for (K key : keys) {
            try {
                results.put(key, new EntryProcessorResultWrapper<>(invoke(key, entryProcessor, arguments)));
            } catch (RuntimeException e) {
                results.put(key, new EntryProcessorResultWrapper<>(new EntryProcessorException(e)));
            }
        }
        return results;
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        throw new UnsupportedOperationException();
    }

    public ByteBuf encodeRedisKey(K key) {
        return toRedisKey(Codecs.encode(keyCodec, key));
    }

    public ByteBuf[] encodeRedisKeys(Collection<? extends K> keys) {
        ByteBuf[] encodedRedisKeys = new ByteBuf[keys.size()];
        try {
            int i = 0;
            for (K key : keys) {
                encodedRedisKeys[i++] = encodeRedisKey(key);
            }
            return encodedRedisKeys;
        } catch (RuntimeException e) {
            Codecs.release(encodedRedisKeys);
            throw e;
        }
    }

    public ByteBuf toRedisKey(ByteBuf key) {
        return Unpooled.wrappedBuffer(keyPrefix.retain(), key);
    }

    protected Map<K, V> toMap(Iterable<? extends K> keys, Iterable<? extends V> values) {
        Map<K, V> map = new LinkedHashMap<>();
        Iterator<? extends V> value = values.iterator();
        for (K key : keys) {
            if (value.hasNext()) {
                map.put(key, value.next());
            } else {
                throw new IllegalStateException();
            }
        }
        return map;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "redis=" + redis +
                ", name=" + configuration.getName() +
                ", keyCodec=" + keyCodec +
                ", valueCodec=" + valueCodec +
                ", expiryForUpdate=" + configuration.getExpiryForUpdate() +
                ", expiryForUpdateTimeUnit=" + configuration.getExpiryForUpdateTimeUnit() +
                ", keyNotificationEnabled=" + configuration.isKeyNotificationEnabled() +
                ", keyTypeCanonicalName=" + Arrays.toString(configuration.getKeyTypeCanonicalName()) +
                ", valueTypeCanonicalName='" + configuration.getValueTypeCanonicalName() + '\'' +
                '}';
    }
}
