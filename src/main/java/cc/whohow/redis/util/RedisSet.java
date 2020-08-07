package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.impl.ArrayType;
import cc.whohow.redis.util.impl.MappingIterator;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 集合、去重队列
 */
public class RedisSet<E> implements Set<E>, Queue<E>, Supplier<Set<E>> {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final ByteBuffer setKey;

    public RedisSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
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

    @Override
    public int size() {
        return (int) scard();
    }

    public long scard() {
        if (log.isTraceEnabled()) {
            log.trace("SCARD {}", toString());
        }
        return redis.scard(setKey.duplicate());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
        return sismember((E) o);
    }

    public boolean sismember(E e) {
        if (log.isTraceEnabled()) {
            log.trace("SISMEMBER {} {}", toString(), e);
        }
        return redis.sismember(setKey.duplicate(), encode(e));
    }

    @Override
    public Iterator<E> iterator() {
        return new MappingIterator<>(new RedisSetIterator(redis, setKey.duplicate()), this::decode);
    }

    @Override
    public Object[] toArray() {
        if (log.isTraceEnabled()) {
            log.trace("SMEMBERS {}", toString());
        }
        return redis.smembers(setKey.duplicate()).stream()
                .map(this::decode)
                .toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray(T[] a) {
        if (log.isTraceEnabled()) {
            log.trace("SMEMBERS {}", toString());
        }
        return redis.smembers(setKey.duplicate()).stream()
                .map(this::decode)
                .toArray(ArrayType.of(a)::newInstance);
    }

    @Override
    public boolean add(E e) {
        if (log.isTraceEnabled()) {
            log.trace("SADD {} {}", toString(), e);
        }
        return redis.sadd(setKey.duplicate(), encode(e)) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        return srem((E) o) > 0;
    }

    public long srem(E e) {
        if (log.isTraceEnabled()) {
            log.trace("SREM {} {}", toString(), e);
        }
        return redis.srem(setKey.duplicate(), encode(e));
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean addAll(Collection<? extends E> c) {
        if (log.isTraceEnabled()) {
            log.trace("SADD {} {}", toString(), c);
        }
        return redis.sadd(setKey.duplicate(), c.stream()
                .map(this::encode)
                .toArray(ByteBuffer[]::new)) > 0;
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeAll(Collection<?> c) {
        if (log.isTraceEnabled()) {
            log.trace("SREM {} {}", toString(), c);
        }
        return redis.srem(setKey.duplicate(), c.stream()
                .map(o -> encode((E) o))
                .toArray(ByteBuffer[]::new)) > 0;
    }

    @Override
    public void clear() {
        if (log.isTraceEnabled()) {
            log.trace("DEL {}", toString());
        }
        redis.del(setKey.duplicate());
    }

    @Override
    public boolean offer(E e) {
        return add(e);
    }

    @Override
    public E remove() {
        return checkElement(poll());
    }

    @Override
    public E poll() {
        if (log.isTraceEnabled()) {
            log.trace("SPOP {}", toString());
        }
        return decode(redis.spop(setKey.duplicate()));
    }

    @Override
    public E element() {
        return checkElement(peek());
    }

    @Override
    public E peek() {
        if (log.isTraceEnabled()) {
            log.trace("SRANDMEMBER {}", toString());
        }
        return decode(redis.srandmember(setKey.duplicate()));
    }

    protected E checkElement(E e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public Set<E> get() {
        if (log.isTraceEnabled()) {
            log.trace("SMEMBERS {}", toString());
        }
        return redis.smembers(setKey.duplicate()).stream()
                .map(this::decode)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
