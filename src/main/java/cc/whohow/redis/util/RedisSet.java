package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.impl.ArrayType;
import cc.whohow.redis.util.impl.MappingIterator;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 集合、去重队列
 */
public class RedisSet<E>
        extends AbstractRedisSet<E>
        implements Set<E>, Queue<E>, Copyable<Set<E>> {
    public RedisSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
        super(redis, codec, key);
    }

    @Override
    public int size() {
        return scard().intValue();
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

    @Override
    public Iterator<E> iterator() {
        return new MappingIterator<>(new RedisSetIterator(redis, setKey.duplicate()), this::decode);
    }

    @Override
    public Object[] toArray() {
        return smembers()
                .toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray(T[] a) {
        return smembers()
                .toArray(ArrayType.of(a)::newInstance);
    }

    @Override
    public boolean add(E e) {
        return sadd(e) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        return srem((E) o) > 0;
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
        return sadd(c) > 0;
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
        return srem((Collection<? extends E>) c) > 0;
    }

    @Override
    public void clear() {
        del();
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
        return spop();
    }

    @Override
    public E element() {
        return checkElement(peek());
    }

    @Override
    public E peek() {
        return srandmember();
    }

    protected E checkElement(E e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public Set<E> copy() {
        return smembers()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
