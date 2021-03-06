package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.Codec;
import io.lettuce.core.ScoredValue;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Redis延迟队列
 */
public class RedisDelayQueue<E>
        extends AbstractRedisSortedSet<E>
        implements BlockingQueue<RedisDelayed<E>>, Copyable<Queue<RedisDelayed<E>>> {
    protected final Clock clock;
    protected volatile long pollInterval = 1000;

    public RedisDelayQueue(Redis redis, Codec<E> codec, String key) {
        this(redis, codec, key, new RedisClock(redis));
    }

    public RedisDelayQueue(Redis redis, Codec<E> codec, String key, Clock clock) {
        super(redis, codec, ByteSequence.utf8(key));
        this.clock = clock;
    }

    protected RedisDelayed<E> decode(ScoredValue<ByteBuffer> scoredValue) {
        return new RedisDelayed<>(codec.decode(scoredValue.getValue()), (long) scoredValue.getScore());
    }

    protected RedisDelayed<E> toDelayed(ScoredValue<E> scoredValue) {
        return new RedisDelayed<>(scoredValue.getValue(), (long) scoredValue.getScore());
    }

    public Clock getClock() {
        return clock;
    }

    public Duration getPollInterval() {
        return Duration.ofMillis(pollInterval);
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval.toMillis();
    }

    public void setPollInterval(long pollInterval, TimeUnit unit) {
        this.pollInterval = unit.toMillis(pollInterval);
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
        RedisDelayed<E> e = (RedisDelayed<E>) o;
        return containsElement(e.getKey());
    }

    public boolean containsElement(E element) {
        return zscore(element) != null;
    }

    @Override
    public int drainTo(Collection<? super RedisDelayed<E>> c) {
        return drainTo(c, 1024);
    }

    @Override
    public int drainTo(Collection<? super RedisDelayed<E>> c, int maxElements) {
        return (int) zremrangebyscoreWithScores(0, clock.millis(), 0, maxElements)
                .stream()
                .map(this::toDelayed)
                .peek(c::add)
                .count();
    }

    @Override
    public Iterator<RedisDelayed<E>> iterator() {
        return new MappingIterator<>(new RedisIterator<>(new RedisSortedSetScanIterator<>(redis, codec::decode, sortedSetKey)), this::toDelayed);
    }

    @Override
    public Object[] toArray() {
        return zrangeWithScores(0, -1)
                .stream()
                .map(this::toDelayed)
                .toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray(T[] a) {
        return zrangeWithScores(0, -1)
                .stream()
                .map(this::toDelayed)
                .toArray(ArrayType.of(a)::newInstance);
    }

    public boolean add(E element, long millis) {
        return add(new RedisDelayed<>(element, millis));
    }

    public boolean add(E element, Instant instant) {
        return add(new RedisDelayed<>(element, instant));
    }

    public boolean add(E element, Date date) {
        return add(new RedisDelayed<>(element, date));
    }

    public boolean add(E element, Duration delay) {
        return add(new RedisDelayed<>(element, delay, clock));
    }

    @Override
    public boolean add(RedisDelayed<E> e) {
        return zadd(e.getDelay(TimeUnit.MILLISECONDS), e.getKey()) > 0;
    }

    public boolean removeElement(E element) {
        return zrem(element) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        RedisDelayed<E> e = (RedisDelayed<E>) o;
        return removeElement(e.getKey());
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends RedisDelayed<E>> c) {
        return zadd(c) > 0;
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        del();
    }

    @Override
    public boolean offer(RedisDelayed<E> e) {
        return add(e);
    }

    @Override
    public void put(RedisDelayed<E> e) throws InterruptedException {
        add(e);
    }

    @Override
    public boolean offer(RedisDelayed<E> e, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(e);
    }

    @Override
    public RedisDelayed<E> take() throws InterruptedException {
        while (true) {
            RedisDelayed<E> value = poll();
            if (value != null) {
                return value;
            }
            await(pollInterval);
        }
    }

    @Override
    public RedisDelayed<E> poll(long timeout, TimeUnit unit) throws InterruptedException {
        long t = System.currentTimeMillis() + unit.toMillis(timeout);
        while (System.currentTimeMillis() < t) {
            RedisDelayed<E> value = poll();
            if (value != null) {
                return value;
            }
            await(pollInterval);
        }
        return null;
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public RedisDelayed<E> remove() {
        RedisDelayed<E> e = poll();
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public RedisDelayed<E> poll() {
        return zremrangebyscoreWithScores(0, clock.millis(), 0, 1)
                .stream()
                .findFirst()
                .map(this::toDelayed)
                .orElse(null);
    }

    @Override
    public RedisDelayed<E> element() {
        RedisDelayed<E> e = peek();
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public RedisDelayed<E> peek() {
        return zrangeWithScores(0, 0)
                .stream()
                .findFirst()
                .filter(v -> v.getScore() < clock.millis())
                .map(this::toDelayed)
                .orElse(null);
    }

    protected void await(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    @Override
    public Queue<RedisDelayed<E>> copy() {
        return zrangeWithScores(0, -1)
                .stream()
                .map(this::toDelayed)
                .collect(Collectors.toCollection(LinkedList::new));
    }
}
