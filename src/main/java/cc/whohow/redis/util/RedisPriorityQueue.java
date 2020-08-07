package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.impl.ArrayType;
import cc.whohow.redis.util.impl.MappingIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class RedisPriorityQueue<E>
        extends RedisSortedSetKey<E>
        implements Queue<Map.Entry<E, Number>>, Supplier<Queue<Map.Entry<E, Number>>> {
    public RedisPriorityQueue(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisPriorityQueue(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
        super(redis, codec, key);
    }

    protected Map.Entry<E, Number> decode(ScoredValue<ByteBuffer> scoredValue) {
        return new AbstractMap.SimpleImmutableEntry<>(decode(scoredValue.getValue()), scoredValue.getScore());
    }

    protected Map.Entry<E, Number> toEntry(ScoredValue<E> scoredValue) {
        return new AbstractMap.SimpleImmutableEntry<>(scoredValue.getValue(), scoredValue.getScore());
    }

    @Override
    public int size() {
        return (int) zcard();
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
    public Iterator<Map.Entry<E, Number>> iterator() {
        return new MappingIterator<>(new RedisSortedSetIterator(redis, zsetKey.duplicate()), this::decode);
    }

    @Override
    public Object[] toArray() {
        return zrangeWithScores(0, -1)
                .map(this::toEntry)
                .toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray(T[] a) {
        return zrangeWithScores(0, -1)
                .map(this::toEntry)
                .toArray(ArrayType.of(a)::newInstance);
    }

    @Override
    public boolean add(Map.Entry<E, Number> e) {
        return offer(e);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        if (o instanceof Map.Entry) {
            Map.Entry<E, Number> e = (Map.Entry<E, Number>) o;
            return zrem(e.getKey()) > 0;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Map.Entry<E, Number>> c) {
        if (c.isEmpty()) {
            return false;
        }
        zadd(c);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeAll(Collection<?> c) {
        return zrem(c.stream()
                .map(e -> (Map.Entry<E, Number>) e)
                .map(Map.Entry::getKey)
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
        zadd(priority.doubleValue(), e);
        return true;
    }

    @Override
    public boolean offer(Map.Entry<E, Number> e) {
        return offer(e.getKey(), e.getValue());
    }

    @Override
    public Map.Entry<E, Number> remove() {
        return checkElement(poll());
    }

    @Override
    public Map.Entry<E, Number> poll() {
        ScoredValue<E> value = zpopminWithScores();
        if (value.hasValue()) {
            return toEntry(value);
        }
        return null;
    }

    @Override
    public Map.Entry<E, Number> element() {
        return checkElement(peek());
    }

    @Override
    public Map.Entry<E, Number> peek() {
        return zrangeWithScores(0, 0)
                .findFirst()
                .map(this::toEntry)
                .orElse(null);
    }

    @Override
    public Queue<Map.Entry<E, Number>> get() {
        return zrangeWithScores(0, -1)
                .map(this::toEntry)
                .collect(Collectors.toCollection(LinkedList::new));
    }

    protected Map.Entry<E, Number> checkElement(Map.Entry<E, Number> e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(zsetKey);
    }
}
