package cc.whohow.redis.messaging;

import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisTracking;
import cc.whohow.redis.codec.Codec;
import cc.whohow.redis.util.RedisList;
import cc.whohow.redis.util.RedisPriority;
import cc.whohow.redis.util.RedisPriorityQueue;
import cc.whohow.redis.util.RedisSet;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * 消息队列工厂
 */
public class RedisMessaging {
    private final Redis redis;
    private final RedisTracking redisTracking;
    private final ExecutorService executor;

    public RedisMessaging(Redis redis,
                          RedisTracking redisTracking,
                          ExecutorService executor) {
        this.redis = redis;
        this.redisTracking = redisTracking;
        this.executor = executor;
    }

    public <E> RedisPollingMessageQueue<E> createQueue(String name, Codec<E> codec, Consumer<E> consumer) {
        return new RedisPollingMessageQueue<>(new RedisList<>(redis, codec, name), name, consumer, executor, redisTracking);
    }

    public <E> RedisPollingMessageQueue<E> createUniqueQueue(String name, Codec<E> codec, Consumer<E> consumer) {
        return new RedisPollingMessageQueue<>(new RedisSet<>(redis, codec, name), name, consumer, executor, redisTracking);
    }

    public <E> RedisPollingMessageQueue<RedisPriority<E>> createPriorityQueue(String name, Codec<E> codec, Consumer<RedisPriority<E>> consumer) {
        return new RedisPollingMessageQueue<>(new RedisPriorityQueue<>(redis, codec, name), name, consumer, executor, redisTracking);
    }
}
