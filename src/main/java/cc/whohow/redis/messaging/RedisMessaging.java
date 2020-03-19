package cc.whohow.redis.messaging;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.RedisKeyspaceEvents;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * 消息队列工厂
 */
public class RedisMessaging {
    private final RedisCommands<ByteBuffer, ByteBuffer> redis;
    private final RedisKeyspaceEvents redisKeyspaceEvents;
    private final ExecutorService executor;

    public RedisMessaging(RedisCommands<ByteBuffer, ByteBuffer> redis,
                          RedisKeyspaceEvents redisKeyspaceEvents,
                          ExecutorService executor) {
        this.redis = redis;
        this.redisKeyspaceEvents = redisKeyspaceEvents;
        this.executor = executor;
    }

    public <E> RedisMessageQueue<E> createQueue(String name, Codec<E> codec, Consumer<E> consumer) {
        return new RedisMessageQueue<>(redis, codec, name, consumer, executor, redisKeyspaceEvents);
    }
}
