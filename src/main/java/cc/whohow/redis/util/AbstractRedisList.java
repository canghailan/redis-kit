package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.stream.Stream;

public class AbstractRedisList<E> {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final ByteBuffer listKey;

    public AbstractRedisList(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
        this.redis = redis;
        this.codec = codec;
        this.listKey = key;
    }

    protected ByteBuffer encode(E value) {
        return codec.encode(value);
    }

    protected E decode(ByteBuffer byteBuffer) {
        return codec.decode(byteBuffer);
    }

    public Long llen() {
        if (log.isTraceEnabled()) {
            log.trace("LLEN {}", toString());
        }
        return redis.llen(listKey.duplicate());
    }

    public E lindex(long index) {
        if (log.isTraceEnabled()) {
            log.trace("LINDEX {} {}", toString(), index);
        }
        return decode(redis.lindex(listKey.duplicate(), index));
    }

    public void lset(long index, E element) {
        if (log.isTraceEnabled()) {
            log.trace("LSET {} {} {}", toString(), index, element);
        }
        redis.lset(listKey.duplicate(), index, encode(element));
    }

    public Stream<E> lrange(long start, long stop) {
        if (log.isTraceEnabled()) {
            log.trace("LRANGE {} {} {}", toString(), start, stop);
        }
        return redis.lrange(listKey.duplicate(), start, stop).stream()
                .map(this::decode);
    }

    public Long lpush(E e) {
        if (log.isTraceEnabled()) {
            log.trace("LPUSH {} {}", toString(), e);
        }
        return redis.lpush(listKey.duplicate(), encode(e));
    }

    public Long rpush(E e) {
        if (log.isTraceEnabled()) {
            log.trace("RPUSH {} {}", toString(), e);
        }
        return redis.rpush(listKey.duplicate(), encode(e));
    }

    public Long rpush(Collection<? extends E> c) {
        if (c.isEmpty()) {
            return 0L;
        }
        if (log.isTraceEnabled()) {
            log.trace("RPUSH {} {}", toString(), c);
        }
        return redis.rpush(listKey.duplicate(), c.stream()
                .map(this::encode)
                .toArray(ByteBuffer[]::new));
    }

    public E lpop() {
        if (log.isTraceEnabled()) {
            log.trace("LPOP {}", toString());
        }
        return decode(redis.lpop(listKey.duplicate()));
    }

    public E rpop() {
        if (log.isTraceEnabled()) {
            log.trace("LPOP {}", toString());
        }
        return decode(redis.rpop(listKey.duplicate()));
    }

    public E blpop(long timeout) {
        if (log.isTraceEnabled()) {
            log.trace("BLPOP {} {}", toString(), timeout);
        }
        return decode(redis.blpop(timeout, listKey.duplicate()).getValueOrElse(null));
    }

    public E brpop(long timeout) {
        if (log.isTraceEnabled()) {
            log.trace("BRPOP {} {}", toString(), timeout);
        }
        return decode(redis.brpop(0, listKey.duplicate()).getValueOrElse(null));
    }

    public Long lrem(int count, E e) {
        if (log.isTraceEnabled()) {
            log.trace("LREM {} {} {}", toString(), count, e);
        }
        return redis.lrem(listKey.duplicate(), count, encode(e));
    }

    public Long del() {
        if (log.isTraceEnabled()) {
            log.trace("DEL {}", toString());
        }
        return redis.del(listKey.duplicate());
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(listKey);
    }
}
