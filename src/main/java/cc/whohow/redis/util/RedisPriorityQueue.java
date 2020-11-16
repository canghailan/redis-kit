package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.Codec;
import io.lettuce.core.ScoredValue;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Redis优先级队列
 */
public class RedisPriorityQueue<E>
        extends AbstractRedisSortedSet<E>
        implements Queue<RedisPriority<E>>, Copyable<Queue<RedisPriority<E>>> {
    public RedisPriorityQueue(Redis redis, Codec<E> codec, String key) {
        this(redis, codec, ByteSequence.utf8(key));
    }

    public RedisPriorityQueue(Redis redis, Codec<E> codec, ByteSequence key) {
        super(redis, codec, key);
    }

    protected RedisPriority<E> toEntry(ScoredValue<E> scoredValue) {
        return new RedisPriority<>(scoredValue.getValue(), scoredValue.getScore());
    }

    @Override
    public int size() {
        return zcard().intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
        if (o instanceof Map.Entry) {
            Map.Entry<E, Number> e = (Map.Entry<E, Number>) o;
            return zscore(e.getKey()) != null;
        }
        return false;
    }

    @Override
    public Iterator<RedisPriority<E>> iterator() {
        return new MappingIterator<>(new RedisIterator<>(new RedisSortedSetScanIterator<>(redis, codec::decode, sortedSetKey)), this::toEntry);
    }

    @Override
    public Object[] toArray() {
        return zrangeWithScores(0, -1)
                .stream()
                .map(this::toEntry)
                .toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray(T[] a) {
        return zrangeWithScores(0, -1)
                .stream()
                .map(this::toEntry)
                .toArray(ArrayType.of(a)::newInstance);
    }

    @Override
    public boolean add(RedisPriority<E> e) {
        return offer(e);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        if (o instanceof RedisPriority) {
            RedisPriority<E> e = (RedisPriority<E>) o;
            return zrem(e.getKey()) > 0;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends RedisPriority<E>> c) {
        if (c.isEmpty()) {
            return false;
        }
        return zadd(c) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeAll(Collection<?> c) {
        return zrem(c.stream()
                .map(e -> (RedisPriority<E>) e)
                .map(RedisPriority::getKey)
                .collect(Collectors.toSet())) > 0;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        del();
    }

    public boolean offer(E e, Number priority) {
        return zadd(priority.doubleValue(), e) > 0;
    }

    @Override
    public boolean offer(RedisPriority<E> e) {
        return offer(e.getKey(), e.getValue());
    }

    @Override
    public RedisPriority<E> remove() {
        return checkElement(poll());
    }

    @Override
    public RedisPriority<E> poll() {
        ScoredValue<E> value = zpopminWithScores();
        if (value.hasValue()) {
            return toEntry(value);
        }
        return null;
    }

    @Override
    public RedisPriority<E> element() {
        return checkElement(peek());
    }

    @Override
    public RedisPriority<E> peek() {
        return zrangeWithScores(0, 0)
                .stream()
                .findFirst()
                .map(this::toEntry)
                .orElse(null);
    }

    protected RedisPriority<E> checkElement(RedisPriority<E> e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public Queue<RedisPriority<E>> copy() {
        return zrangeWithScores(0, -1)
                .stream()
                .map(this::toEntry)
                .collect(Collectors.toCollection(LinkedList::new));
    }
}
