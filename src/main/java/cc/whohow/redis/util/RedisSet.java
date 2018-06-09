package cc.whohow.redis.util;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.StringCodec;
import io.lettuce.core.api.sync.RedisCommands;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 集合、缓冲队列
 */
public class RedisSet<E> implements Set<E>, Queue<E> {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final String id;
    protected final ByteBuffer encodedId;

    public RedisSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String id) {
        this.redis = redis;
        this.codec = codec;
        this.id = id;
        this.encodedId = StringCodec.UTF_8.encode(id);
    }

    public ByteBuffer encode(E value) {
        return codec.encode(value);
    }

    public E decode(ByteBuffer buffer) {
        return codec.decode(buffer);
    }

    @Override
    public int size() {
        return redis.scard(encodedId.duplicate()).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
        return redis.sismember(encodedId.duplicate(), encode((E) o));
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
        return redis.smembers(encodedId.duplicate()).stream()
                .map(this::decode)
                .toArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        return redis.smembers(encodedId.duplicate()).stream()
                .map(this::decode)
                .toArray((length) -> (T[]) Array.newInstance(a.getClass().getComponentType(), length));
    }

    @Override
    public boolean add(E e) {
        return redis.sadd(encodedId.duplicate(), encode(e)) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        return redis.srem(encodedId.duplicate(), encode((E) o)) > 0;
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        ByteBuffer[] encoded = c.stream()
                .map(this::encode)
                .toArray(ByteBuffer[]::new);
        return redis.sadd(encodedId.duplicate(), encoded) > 0;
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
        ByteBuffer[] encoded = c.stream()
                .map((e) -> (E) e)
                .map(this::encode)
                .toArray(ByteBuffer[]::new);
        return redis.srem(encodedId.duplicate(), encoded) > 0;
    }

    @Override
    public void clear() {
        redis.del(encodedId.duplicate());
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
        return decode(redis.spop(encodedId.duplicate()));
    }

    @Override
    public E element() {
        return checkElement(peek());
    }

    @Override
    public E peek() {
        return decode(redis.srandmember(encodedId.duplicate()));
    }

    protected E checkElement(E e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public String toString() {
        return id;
    }
}
