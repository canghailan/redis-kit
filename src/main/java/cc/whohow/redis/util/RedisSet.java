package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * 集合、缓冲队列
 */
public class RedisSet<E> implements Set<E>, Queue<E> {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final ByteBuffer key;

    public RedisSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
        this.redis = redis;
        this.codec = codec;
        this.key = key;
    }

    protected ByteBuffer encode(E value) {
        return codec.encode(value);
    }

    protected E decode(ByteBuffer byteBuffer) {
        return codec.decode(byteBuffer);
    }

    @Override
    public int size() {
        return redis.scard(key.duplicate()).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
        return redis.sismember(key.duplicate(), encode((E) o));
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        return redis.smembers(key.duplicate()).toArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        return redis.smembers(key.duplicate()).toArray(a);
    }

    @Override
    public boolean add(E e) {
        return redis.sadd(key.duplicate(), encode(e)) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        return redis.srem(key.duplicate(), encode((E) o)) > 0;
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
        return redis.sadd(key.duplicate(), c.stream().map(this::encode).toArray(ByteBuffer[]::new)) > 0;
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
        return redis.srem(key.duplicate(), c.stream().map(o -> encode((E) o)).toArray(ByteBuffer[]::new)) > 0;
    }

    @Override
    public void clear() {
        redis.del(key.duplicate());
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
        return decode(redis.spop(key.duplicate()));
    }

    @Override
    public E element() {
        return checkElement(peek());
    }

    @Override
    public E peek() {
        return decode(redis.srandmember(key.duplicate()));
    }

    protected E checkElement(E e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }
}
