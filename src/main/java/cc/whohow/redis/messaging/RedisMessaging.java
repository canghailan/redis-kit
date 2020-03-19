package cc.whohow.redis.messaging;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.RedisKeyspaceEvent;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * 消息队列工厂
 */
public class RedisMessaging {
    private final RedisCommands<ByteBuffer, ByteBuffer> redis;
    private final RedisKeyspaceEvent redisKeyspaceEvent;
    private final ExecutorService executor;

    public RedisMessaging(RedisCommands<ByteBuffer, ByteBuffer> redis,
                          RedisKeyspaceEvent redisKeyspaceEvent,
                          ExecutorService executor) {
        this.redis = redis;
        this.redisKeyspaceEvent = redisKeyspaceEvent;
        this.executor = executor;
    }

    public <E> RedisMessageQueue<E> createQueue(String name, Codec<E> codec, Consumer<E> consumer) {
        return new RedisMessageQueue<>(redis, codec, name, consumer, executor, redisKeyspaceEvent);
    }
}