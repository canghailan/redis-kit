package cc.whohow.redis;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.DefaultCodecFactory;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.script.RedisScriptCommands;
import cc.whohow.redis.util.*;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Function;
import java.util.function.Supplier;

public class RedisFactory implements Supplier<RedisCommands<ByteBuffer, ByteBuffer>>,
        RedisAtomicFactory,
        RedisClockFactory,
        RedisLockFactory, AutoCloseable {
    private final RedisURI redisURI;
    private final RedisClient redisClient;
    private final StatefulRedisConnection<ByteBuffer, ByteBuffer> redisConnection;
    private final Function<Class<?>, Codec<?>> codecFactory = new DefaultCodecFactory();
    private final RedisScriptCommands script;

    public RedisFactory(RedisClient redisClient, RedisURI redisURI) {
        this.redisClient = redisClient;
        this.redisURI = redisURI;
        this.redisConnection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
        this.script = new RedisScriptCommands(redisConnection.sync());
    }

    @Override
    public RedisCommands<ByteBuffer, ByteBuffer> get() {
        return redisConnection.sync();
    }

    public RedisScriptCommands script() {
        return script;
    }

    @SuppressWarnings("unchecked")
    public <T> Codec<T> newCodec(Class<T> type) {
        return (Codec<T>) codecFactory.apply(type);
    }

    public Clock clock(ZoneId zone) {
        return new RedisClock(get(), zone);
    }

    public <T> RedisList<T> newList(String key, Class<T> type) {
        return new RedisList<>(get(), newCodec(type), key);
    }

    public <K, V> RedisMap<K, V> newMap(String key, Class<K> keyType, Class<V> valueType) {
        return new RedisMap<>(get(), newCodec(keyType), newCodec(valueType), key);
    }

    public <T> RedisSet<T> newSet(String key, Class<T> type) {
        return new RedisSet<>(get(), newCodec(type), key);
    }

    public <T> RedisSortedSet<T> newSortedSet(String key, Class<T> type) {
        return new RedisSortedSet<>(get(), newCodec(type), key);
    }

    public RedisLock newLock(String key, Duration maxLockTime) {
        return new RedisLock(get(), key, maxLockTime);
    }

    public RedisLock newLock(String key, Duration minLockTime, Duration maxLockTime) {
        return new RedisLock(get(), key, minLockTime, maxLockTime);
    }

    @Override
    public RedisAtomicLong newAtomicLong(String key) {
        return new RedisAtomicLong(get(), key);
    }

    @Override
    public <T> RedisAtomicReference<T> newAtomicReference(String name, Class<T> type) {
        return new RedisAtomicReference<>(get(), newCodec(type), name);
    }

    public RedisKeyspaceNotification newRedisKeyspaceNotification() {
        return new RedisKeyspaceNotification(redisClient, redisURI);
    }

    public <T> RedisPriorityQueue<T> newPriorityQueue(String name, Class<T> type) {
        return new RedisPriorityQueue<T>(get(), newCodec(type), name);
    }

    public <T> RedisDelayQueue<T> newDelayQueue(String name, Class<T> type) {
        return new RedisDelayQueue<T>(get(), newCodec(type), name);
    }

    @Override
    public void close() throws Exception {
        redisConnection.close();
    }
}
