package cc.whohow.redis.util;

import cc.whohow.redis.RedisTracking;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.lettuce.ByteSequenceRedisCodec;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Redis键空间事件处理
 */
public class RedisKeyspaceNotification implements
        RedisTracking,
        RedisPubSubListener<ByteSequence, ByteSequence> {
    protected static final Logger log = LogManager.getLogger();
    protected final ByteSequence keyspace;
    protected final Map<RedisKeyPattern, Set<Listener>> listeners = new ConcurrentHashMap<>();
    protected volatile StatefulRedisPubSubConnection<ByteSequence, ByteSequence> redisPubSubConnection;

    public RedisKeyspaceNotification(RedisClient redisClient, RedisURI redisURI) {
        String keyspace = "__keyspace@" + redisURI.getDatabase() + "__:";
        this.keyspace = ByteSequence.ascii(keyspace);
        this.redisPubSubConnection = redisClient.connectPubSub(ByteSequenceRedisCodec.get(), redisURI);
        this.redisPubSubConnection.addListener(this);
        this.redisPubSubConnection.sync().psubscribe(ByteSequence.ascii(keyspace + "*"));
    }

    public void addListener(String pattern, RedisTracking.Listener listener) {
        addListener(ByteSequence.utf8(pattern), listener);
    }

    @Override
    public void addListener(ByteSequence pattern, RedisTracking.Listener listener) {
        listeners.computeIfAbsent(new RedisKeyPattern(pattern), key -> new CopyOnWriteArraySet<>())
                .add(listener);
    }

    public void removeListener(String pattern, RedisTracking.Listener listener) {
        removeListener(ByteSequence.utf8(pattern), listener);
    }

    @Override
    public void removeListener(ByteSequence pattern, RedisTracking.Listener listener) {
        listeners.getOrDefault(new RedisKeyPattern(pattern), Collections.emptySet())
                .remove(listener);
    }

    public void onKeyEvent(ByteSequence key) {
        RedisKeyPattern keyPattern = new RedisKeyPattern(key, false);
        for (Map.Entry<RedisKeyPattern, Set<Listener>> e : listeners.entrySet()) {
            if (e.getKey().match(keyPattern)) {
                for (Listener listener : e.getValue()) {
                    try {
                        listener.onInvalidate(key);
                    } catch (Throwable ex) {
                        log.warn("onKeyEventError", ex);
                    }
                }
            }
        }
    }

    @Override
    public void message(ByteSequence channel, ByteSequence message) {
        log.trace("message {} {}", channel, message);
        onKeyEvent(channel.subSequence(keyspace.length(), channel.length()));
    }

    @Override
    public void message(ByteSequence pattern, ByteSequence channel, ByteSequence message) {
        log.trace("message {} {}", channel, message);
        onKeyEvent(channel.subSequence(keyspace.length(), channel.length()));
    }

    @Override
    public void subscribed(ByteSequence channel, long count) {
        log.trace("subscribed {} {}", channel, count);
    }

    @Override
    public void psubscribed(ByteSequence pattern, long count) {
        log.trace("psubscribed {} {}", pattern, count);
    }

    @Override
    public void unsubscribed(ByteSequence channel, long count) {
        log.trace("unsubscribed {} {}", channel, count);
    }

    @Override
    public void punsubscribed(ByteSequence pattern, long count) {
        log.trace("punsubscribed {} {}", pattern, count);
    }

    @Override
    public void close() throws Exception {
        if (redisPubSubConnection != null) {
            redisPubSubConnection.close();
            redisPubSubConnection = null;
        }
    }

    @Override
    public String toString() {
        return keyspace.toString();
    }
}
