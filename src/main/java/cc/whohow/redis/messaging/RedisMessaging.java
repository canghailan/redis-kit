package cc.whohow.redis.messaging;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.*;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * 消息队列工厂
 */
public class RedisMessaging {
    private final RedisCommands<ByteBuffer, ByteBuffer> redis;
    private final RedisKeyspaceNotification redisKeyspaceNotification;
    private final ExecutorService executor;

    public RedisMessaging(RedisCommands<ByteBuffer, ByteBuffer> redis,
                          RedisKeyspaceNotification redisKeyspaceNotification,
                          ExecutorService executor) {
        this.redis = redis;
        this.redisKeyspaceNotification = redisKeyspaceNotification;
        this.executor = executor;
    }

    public <E> RedisPollingMessageQueue<E> createQueue(String name, Codec<E> codec, Consumer<E> consumer) {
        return new RedisPollingMessageQueue<>(new RedisList<>(redis, codec, name), name, consumer, executor, redisKeyspaceNotification);
    }

    public <E> RedisPollingMessageQueue<E> createUniqueQueue(String name, Codec<E> codec, Consumer<E> consumer) {
        return new RedisPollingMessageQueue<>(new RedisSet<>(redis, codec, name), name, consumer, executor, redisKeyspaceNotification);
    }

    public <E> RedisPollingMessageQueue<RedisPriority<E>> createPriorityQueue(String name, Codec<E> codec, Consumer<RedisPriority<E>> consumer) {
        return new RedisPollingMessageQueue<>(new RedisPriorityQueue<>(redis, codec, name), name, consumer, executor, redisKeyspaceNotification);
    }
}
