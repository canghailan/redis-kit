package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.impl.MappingIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Supplier;

public class RedisPriorityQueue<E> implements Queue<Map.Entry<E, Number>>, Supplier<Queue<Map.Entry<E, Number>>> {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final ByteBuffer key;

    public RedisPriorityQueue(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisPriorityQueue(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
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
        return redis.zcard(key.duplicate()).intValue();
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
            return redis.zscore(key.duplicate(), encode(e.getKey())) != null;
        }
        return false;
    }

    @Override
    public Iterator<Map.Entry<E, Number>> iterator() {
        return new MappingIterator<>(
                new RedisSortedSetIterator(redis, key.duplicate()),
                (value) -> new AbstractMap.SimpleImmutableEntry<>(decode(value.getValue()), value.getScore()));
    }

    @Override
    public Object[] toArray() {
        return get().toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray(T[] a) {
        return get().toArray(a);
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
            return redis.zrem(key.duplicate(), encode(e.getKey())) > 0;
        }
        return false;
    }

    public boolean removeIfKey(E k) {
        return redis.zrem(key.duplicate(), encode(k)) > 0;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean addAll(Collection<? extends Map.Entry<E, Number>> c) {
        if (c.isEmpty()) {
            return false;
        }
        redis.zadd(key.duplicate(), c.stream()
                .map(e -> ScoredValue.fromNullable(e.getValue().doubleValue(), encode(e.getKey())))
                .toArray(ScoredValue[]::new));
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeAll(Collection<?> c) {
        return redis.zrem(key.duplicate(), c.stream()
                .map(e -> (Map.Entry<E, Number>) e)
                .map(Map.Entry::getKey)
                .map(this::encode)
                .toArray(ByteBuffer[]::new)) > 0;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        redis.del(key.duplicate());
    }

    public boolean offer(E e, Number priority) {
        redis.zadd(key.duplicate(), priority.doubleValue(), encode(e));
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
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Map.Entry<E, Number> element() {
        return checkElement(peek());
    }

    @Override
    public Map.Entry<E, Number> peek() {
        return redis.zrangeWithScores(key.duplicate(), 0, 0).stream()
                .findFirst()
                .map(v -> new AbstractMap.SimpleImmutableEntry<E, Number>(decode(v.getValue()), v.getScore()))
                .orElse(null);
    }

    @Override
    public Queue<Map.Entry<E, Number>> get() {
        return redis.zrangeWithScores(key.duplicate(), 0, -1).stream()
                .map(v -> new AbstractMap.SimpleImmutableEntry<E, Number>(decode(v.getValue()), v.getScore()))
                .collect(LinkedList::new, LinkedList::addLast, LinkedList::addAll);
    }

    protected Map.Entry<E, Number> checkElement(Map.Entry<E, Number> e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }
}
