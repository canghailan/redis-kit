package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.stream.Stream;

public class AbstractRedisSet<E> {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final ByteBuffer setKey;

    public AbstractRedisSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
        this.redis = redis;
        this.codec = codec;
        this.setKey = key;
    }

    protected ByteBuffer encode(E value) {
        return codec.encode(value);
    }

    protected E decode(ByteBuffer byteBuffer) {
        return codec.decode(byteBuffer);
    }

    public Long scard() {
        if (log.isTraceEnabled()) {
            log.trace("SCARD {}", toString());
        }
        return redis.scard(setKey.duplicate());
    }

    public boolean sismember(E e) {
        if (log.isTraceEnabled()) {
            log.trace("SISMEMBER {} {}", toString(), e);
        }
        return redis.sismember(setKey.duplicate(), encode(e));
    }

    public Stream<E> smembers() {
        if (log.isTraceEnabled()) {
            log.trace("SMEMBERS {}", toString());
        }
        return redis.smembers(setKey.duplicate()).stream()
                .map(this::decode);
    }

    public E srandmember() {
        if (log.isTraceEnabled()) {
            log.trace("SRANDMEMBER {}", toString());
        }
        return decode(redis.srandmember(setKey.duplicate()));
    }

    public Long sadd(E e) {
        if (log.isTraceEnabled()) {
            log.trace("SADD {} {}", toString(), e);
        }
        return redis.sadd(setKey.duplicate(), encode(e));
    }

    public Long sadd(Collection<? extends E> c) {
        if (log.isTraceEnabled()) {
            log.trace("SADD {} {}", toString(), c);
        }
        return redis.sadd(setKey.duplicate(), c.stream()
                .map(this::encode)
                .toArray(ByteBuffer[]::new));
    }

    public Long srem(E e) {
        if (log.isTraceEnabled()) {
            log.trace("SREM {} {}", toString(), e);
        }
        return redis.srem(setKey.duplicate(), encode(e));
    }

    public Long srem(Collection<? extends E> c) {
        if (log.isTraceEnabled()) {
            log.trace("SREM {} {}", toString(), c);
        }
        return redis.srem(setKey.duplicate(), c.stream()
                .map(this::encode)
                .toArray(ByteBuffer[]::new));
    }

    public E spop() {
        if (log.isTraceEnabled()) {
            log.trace("SPOP {}", toString());
        }
        return decode(redis.spop(setKey.duplicate()));
    }

    public Long del() {
        if (log.isTraceEnabled()) {
            log.trace("DEL {}", toString());
        }
        return redis.del(setKey.duplicate());
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(setKey);
    }
}
