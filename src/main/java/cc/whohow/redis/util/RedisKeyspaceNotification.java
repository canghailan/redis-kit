package cc.whohow.redis.util;

import cc.whohow.redis.RedisTracking;
import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.lettuce.ByteSequenceRedisCodec;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Redis键空间事件处理
 */
public class RedisKeyspaceNotification implements
        RedisTracking,
        RedisPubSubListener<ByteSequence, ByteSequence> {
    protected static final Logger log = LogManager.getLogger();
    protected final ByteSequence keyspace;
    protected final NavigableMap<MatchKey, Set<Listener>> listeners = new ConcurrentSkipListMap<>();
    protected volatile StatefulRedisPubSubConnection<ByteSequence, ByteSequence> redisPubSubConnection;

    public RedisKeyspaceNotification(RedisClient redisClient, RedisURI redisURI) {
        String keyspace = "__keyspace@" + redisURI.getDatabase() + "__:";
        this.keyspace = ByteSequence.ascii(keyspace);
        this.redisPubSubConnection = redisClient.connectPubSub(ByteSequenceRedisCodec.get(), redisURI);
        this.redisPubSubConnection.addListener(this);
        this.redisPubSubConnection.sync().psubscribe(ByteSequence.ascii(keyspace + "*"));
    }
//
//    public void addListener(String key, Listener listener) {
//        addListener(new MatchKey(ByteBuffers.fromUtf8(key)), listener);
//    }
//
//    public void addListener(ByteBuffer key, Listener listener) {
//        addListener(new MatchKey(ByteBuffers.copy(key)), listener);
//    }
//
//    private void addListener(MatchKey key, Listener listener) {
//        listeners.computeIfAbsent(key, (k) -> new CopyOnWriteArraySet<>())
//                .add(listener);
//    }
//
//    public void removeListener(String key, Listener listener) {
//        removeListener(new MatchKey(ByteBuffers.fromUtf8(key)), listener);
//    }
//
//    public void removeListener(ByteBuffer key, Listener listener) {
//        removeListener(new MatchKey(ByteBuffers.copy(key)), listener);
//    }
//
//    public void removeListener(MatchKey key, Listener listener) {
//        Set<Listener> set = listeners.get(key);
//        if (set != null) {
//            set.remove(listener);
//        }
//    }
//
//    protected ByteBuffer toKey(ByteBuffer channel) {
//        channel.position(channel.position() + keyOffset);
//        return channel;
//    }
//
//    protected ByteBuffer getStartKey(ByteBuffer key) {
//        return ByteBuffer.wrap(new byte[]{key.get(key.position()), '*'});
//    }
//
//    protected void onEvent(ByteBuffer channel, ByteBuffer command) {
//        try {
//            ByteBuffer key = toKey(channel);
//            MatchKey matchKey = new MatchKey(key, false);
//            MatchKey startMatchKey = new MatchKey(getStartKey(key));
//
//            log.trace("onKeyEvent {}", matchKey);
//
//            int keyP = key.position();
//            int keyL = key.limit();
//            int commandP = command.position();
//            int commandL = command.limit();
//            for (Map.Entry<MatchKey, Set<Listener>> e : listeners.subMap(
//                    startMatchKey, true,
//                    matchKey, true).entrySet()) {
//                if (e.getKey().match(matchKey)) {
//                    for (Listener listener : e.getValue()) {
//                        try {
//                            key.limit(keyL).position(keyP);
//                            command.limit(commandL).position(commandP);
//                            listener.onKeyEvent(key, command);
//                        } catch (Throwable ex) {
//                            log.error("keyspace event listener error", ex);
//                        }
//                    }
//                }
//            }
//        } catch (Throwable ex) {
//            log.error("keyspace event process error", ex);
//        }
//    }
//
//    @Override
//    public void message(ByteBuffer channel, ByteBuffer message) {
//        onEvent(channel, message);
//    }
//
//    @Override
//    public void message(ByteBuffer pattern, ByteBuffer channel, ByteBuffer message) {
//        onEvent(channel, message);
//    }
//
//    @Override
//    public void subscribed(ByteBuffer channel, long count) {
//        for (Set<Listener> list : listeners.values()) {
//            for (Listener listener : list) {
//                try {
//                    listener.subscribed();
//                } catch (Throwable e) {
//                    log.error(e.getMessage(), e);
//                }
//            }
//        }
//    }
//
//    @Override
//    public void psubscribed(ByteBuffer pattern, long count) {
//        for (Set<Listener> list : listeners.values()) {
//            for (Listener listener : list) {
//                try {
//                    listener.subscribed();
//                } catch (Throwable e) {
//                    log.error(e.getMessage(), e);
//                }
//            }
//        }
//    }
//
//    @Override
//    public void unsubscribed(ByteBuffer channel, long count) {
//        for (Set<Listener> list : listeners.values()) {
//            for (Listener listener : list) {
//                try {
//                    listener.unsubscribed();
//                } catch (Throwable e) {
//                    log.error(e.getMessage(), e);
//                }
//            }
//        }
//    }
//
//    @Override
//    public void punsubscribed(ByteBuffer pattern, long count) {
//        for (Set<Listener> list : listeners.values()) {
//            for (Listener listener : list) {
//                try {
//                    listener.unsubscribed();
//                } catch (Throwable e) {
//                    log.error(e.getMessage(), e);
//                }
//            }
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//        if (redisPubSubConnection != null) {
//            redisPubSubConnection.close();
//            redisPubSubConnection = null;
//        }
//    }
//
//    @Override
//    public String toString() {
//        return keyspace;
//    }

    public void addListener(String pattern, RedisTracking.Listener listener) {
        addListener(ByteSequence.utf8(pattern), listener);
    }

    @Override
    public void addListener(ByteSequence pattern, RedisTracking.Listener listener) {

    }

    public void removeListener(String pattern, RedisTracking.Listener listener) {
        removeListener(ByteSequence.utf8(pattern), listener);
    }

    @Override
    public void removeListener(ByteSequence pattern, RedisTracking.Listener listener) {

    }

    public void onKeyEvent(ByteSequence key) {

    }

    @Override
    public void message(ByteSequence channel, ByteSequence message) {
        onKeyEvent(channel.subSequence(keyspace.length(), channel.length()));
    }

    @Override
    public void message(ByteSequence pattern, ByteSequence channel, ByteSequence message) {
        onKeyEvent(channel.subSequence(keyspace.length(), channel.length()));
    }

    @Override
    public void subscribed(ByteSequence channel, long count) {
    }

    @Override
    public void psubscribed(ByteSequence pattern, long count) {
    }

    @Override
    public void unsubscribed(ByteSequence channel, long count) {
    }

    @Override
    public void punsubscribed(ByteSequence pattern, long count) {
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

    private static class MatchKey implements Comparable<MatchKey> {
        protected final boolean pattern;
        protected final ByteBuffer key;
        protected final int hashCode;

        private MatchKey(ByteBuffer key) {
            this(key, true);
        }

        private MatchKey(ByteBuffer key, boolean detect) {
            this.hashCode = key.hashCode();
            if (detect) {
                this.pattern = key.get(key.remaining() - 1) == '*';
                if (this.pattern) {
                    key.limit(key.limit() - 1);
                }
            } else {
                this.pattern = false;
            }
            this.key = key;
        }

        public boolean match(MatchKey that) {
            if (pattern) {
                return ByteBuffers.startsWith(that.key, key);
            } else {
                return hashCode == that.hashCode
                        && key.equals(that.key);
            }
        }

        @Override
        public int compareTo(MatchKey o) {
            int c = key.compareTo(o.key);
            if (c == 0) {
                return -Boolean.compare(pattern, o.pattern);
            }
            return c;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof MatchKey) {
                MatchKey that = (MatchKey) o;
                return hashCode == that.hashCode
                        && pattern == that.pattern
                        && key.equals(that.key);
            }
            return false;
        }

        @Override
        public String toString() {
            if (pattern) {
                return ByteBuffers.toString(key) + "*";
            } else {
                return ByteBuffers.toString(key);
            }
        }
    }
}
