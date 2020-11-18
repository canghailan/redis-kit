package cc.whohow.redis.jcache;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.Codec;
import cc.whohow.redis.jcache.codec.RedisCacheCodecFactory;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.jcache.processor.EntryProcessorResultWrapper;
import cc.whohow.redis.lettuce.*;
import cc.whohow.redis.util.*;
import io.lettuce.core.protocol.CommandType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 普通Redis缓存，不支持过期时间
 */
public class RedisCache<K, V> implements Cache<K, V> {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCacheManager cacheManager;
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final Codec<K> keyCodec;
    protected final Codec<V> valueCodec;
    protected final Redis redis;
    protected final CacheStats cacheStats = new CacheStats();

    public RedisCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration) {
        Objects.requireNonNull(configuration.getName());
        RedisCacheCodecFactory redisCacheCodecFactory = cacheManager.newRedisCacheCodecFactory(configuration);
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        this.keyCodec = redisCacheCodecFactory.newKeyCodec(configuration);
        this.valueCodec = redisCacheCodecFactory.newValueCodec(configuration);
        this.redis = cacheManager.getRedis();
    }

    public Codec<K> getKeyCodec() {
        return keyCodec;
    }

    public Codec<V> getValueCodec() {
        return valueCodec;
    }

    protected CacheValue<V> decodeCacheValue(ByteBuffer byteBuffer) {
        return new ImmutableCacheValue<>(valueCodec.decode(byteBuffer));
    }

    @Override
    public V get(K key) {
        V value = redis.send(new DecodeOutput<>(valueCodec::decode), CommandType.GET, keyCodec.encode(key));
        if (value != null) {
            cacheStats.cacheHit(1);
            return value;
        } else {
            cacheStats.cacheMiss(1);
            return null;
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }
        List<ByteSequence> args = keys.stream()
                .map(keyCodec::encode)
                .collect(Collectors.toList());
        List<V> values = redis.send(new ListOutput<>(valueCodec::decode), CommandType.MGET, args);
        return new KeyValues<>(keys, values);
    }

    @Override
    public boolean containsKey(K key) {
        return redis.send(new IntegerOutput(), CommandType.EXISTS, keyCodec.encode(key)) > 0;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) {
        redis.send(new VoidOutput(), CommandType.SET, keyCodec.encode(key), valueCodec.encode(value));
        cacheStats.cachePut(1);
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = redis.send(new DecodeOutput<>(valueCodec::decode), CommandType.GETSET, keyCodec.encode(key), valueCodec.encode(value));
        if (oldValue != null) {
            cacheStats.cacheHit(1);
        } else {
            cacheStats.cacheMiss(1);
        }
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        if (map.isEmpty()) {
            return;
        }
        List<ByteSequence> args = new ArrayList<>(map.size() * 2);
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            args.add(keyCodec.encode(e.getKey()));
            args.add(valueCodec.encode(e.getValue()));
        }
        redis.send(new VoidOutput(), CommandType.MSET, args);
        cacheStats.cachePut(map.size());
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        boolean ok = RESP.ok(redis.send(new StatusOutput(), CommandType.SET, keyCodec.encode(key), valueCodec.encode(value), RESP.nx()));
        if (ok) {
            cacheStats.cachePut(1);
        }
        return ok;
    }

    @Override
    public boolean remove(K key) {
        boolean ok = redis.send(new IntegerOutput(), CommandType.DEL, keyCodec.encode(key)) > 0;
        if (ok) {
            cacheStats.cacheRemove(1);
        }
        return ok;
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
        boolean ok = RESP.ok(redis.send(new StatusOutput(), CommandType.SET, keyCodec.encode(key), valueCodec.encode(value), RESP.xx()));
        if (ok) {
            cacheStats.cachePut(1);
        }
        return ok;
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        if (keys.isEmpty()) {
            return;
        }
        List<ByteSequence> args = keys.stream()
                .map(keyCodec::encode)
                .collect(Collectors.toList());
        long n = redis.send(new IntegerOutput(), CommandType.DEL, args);
        cacheStats.cacheRemove((int) n);
    }

    @Override
    public void removeAll() {
        RedisKeyScanIterator<ByteSequence> iterator = new RedisKeyScanIterator<>(
                redis, ByteSequence::copy, configuration.getRedisKeyPattern(), 0);

        while (iterator.hasNext()) {
            RedisScanIteration<ByteSequence> iteration = iterator.next();
            if (!iteration.getArray().isEmpty()) {
                redis.send(new VoidOutput(), CommandType.DEL, iteration.getArray());
                cacheStats.cacheRemove(iteration.getArray().size());
            }
        }
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
        return entryProcessor.process(new MutableCacheEntry<>(this, key), arguments);
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
        log.info("close cache: {}", getName());
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
        return new MappingIterator<>(new RedisIterator<>(new RedisKeyScanIterator<>(
                redis, keyCodec::decode, configuration.getRedisKeyPattern(), 0)), this::getEntry);
    }

    @Override
    public V get(K key, CacheLoader<K, ? extends V> cacheLoader) {
        return getValue(key, cacheLoader).get();
    }

    @Override
    public CacheValue<V> getValue(K key) {
        CacheValue<V> cacheValue = redis.send(
                new DecodeOutput<>(this::decodeCacheValue, ImmutableCacheValue.empty()), CommandType.GET, keyCodec.encode(key));
        if (cacheValue != null) {
            cacheStats.cacheHit(1);
        } else {
            cacheStats.cacheMiss(1);
        }
        return cacheValue;
    }

    @Override
    public CacheValue<V> getValue(K key, CacheLoader<K, ? extends V> cacheLoader) {
        CacheValue<V> cacheValue = redis.send(
                new DecodeOutput<>(this::decodeCacheValue, ImmutableCacheValue.empty()), CommandType.GET, keyCodec.encode(key));
        if (cacheValue != null) {
            cacheStats.cacheHit(1);
            return cacheValue;
        } else {
            cacheStats.cacheMiss(1);
            V value = cacheLoader.load(key);
            put(key, value);
            return new ImmutableCacheValue<>(value);
        }
    }

    @Override
    public CacheStatisticsMXBean getCacheStatistics() {
        return cacheStats;
    }

    public Cache.Entry<K, V> getEntry(K key) {
        return new MutableCacheEntry<>(this, key);
    }

    @Override
    public String toString() {
        return getName();
    }
}
