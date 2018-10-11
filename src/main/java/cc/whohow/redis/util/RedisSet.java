package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.lettuce.RedisCommandsAdapter;
import cc.whohow.redis.lettuce.RedisValueCodecAdapter;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * 集合、缓冲队列
 */
public class RedisSet<E> implements Set<E>, Queue<E> {
    protected final RedisCommands<ByteBuffer, E> redis;
    protected final ByteBuffer key;

    public RedisSet(RedisCommands<ByteBuffer, E> redis, ByteBuffer key) {
        this.redis = redis;
        this.key = key;
    }

    public RedisSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(new RedisCommandsAdapter<>(redis, new RedisValueCodecAdapter<>(codec)), ByteBuffers.fromUtf8(key));
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
        return redis.sismember(key.duplicate(), (E) o);
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
        return redis.sadd(key.duplicate(), e) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        return redis.srem(key.duplicate(), (E) o) > 0;
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
        return redis.sadd(key.duplicate(), (E[]) c.toArray()) > 0;
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
        return redis.srem(key.duplicate(), (E[]) c.toArray()) > 0;
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
        return redis.spop(key.duplicate());
    }

    @Override
    public E element() {
        return checkElement(peek());
    }

    @Override
    public E peek() {
        return redis.srandmember(key.duplicate());
    }

    protected E checkElement(E e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }
}
