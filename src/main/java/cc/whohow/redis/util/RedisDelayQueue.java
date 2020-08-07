package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.script.RedisScriptCommands;
import cc.whohow.redis.util.impl.ArrayType;
import cc.whohow.redis.util.impl.MappingIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Redis延迟队列
 */
public class RedisDelayQueue<E>
        extends RedisSortedSetKey<E>
        implements BlockingQueue<RedisDelayed<E>> {
    private static final Logger log = LogManager.getLogger();
    protected final Clock clock;
    protected volatile long pollInterval = 1000;

    public RedisDelayQueue(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, key, new RedisClock(redis));
    }

    public RedisDelayQueue(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key, Clock clock) {
        super(redis, codec, ByteBuffers.fromUtf8(key));
        this.clock = clock;
    }

    protected ByteBuffer encode(E value) {
        return codec.encode(value);
    }

    protected E decode(ByteBuffer byteBuffer) {
        return codec.decode(byteBuffer);
    }

    protected RedisDelayed<E> decode(ScoredValue<ByteBuffer> scoredValue) {
        return new RedisDelayed<>(decode(scoredValue.getValue()), (long) scoredValue.getScore());
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
        return (int) zcard();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
        return zscore((E) o) != null;
    }

    @Override
    public int drainTo(Collection<? super RedisDelayed<E>> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super RedisDelayed<E>> c, int maxElements) {
        long time = clock.millis();
        return (int) zremrangebyscoreWithScores(0, time, 0, maxElements)
                .map(this::toDelayed)
                .peek(c::add)
                .count();
    }

    @Override
    public Iterator<RedisDelayed<E>> iterator() {
        return new MappingIterator<>(new RedisSortedSetIterator(redis, zsetKey.duplicate()), this::decode);
    }

    @Override
    public Object[] toArray() {
        return zrangeWithScores(0, -1)
                .map(this::toDelayed)
                .toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray(T[] a) {
        return zrangeWithScores(0, -1)
                .map(this::toDelayed)
                .toArray(ArrayType.of(a)::newInstance);
    }

    @Override
    public boolean add(RedisDelayed<E> e) {
        return zadd(e.getDelay(TimeUnit.MILLISECONDS), e.getKey()) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        RedisDelayed<E> e = (RedisDelayed<E>) o;
        return zrem(e.getKey()) > 0;
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
        long time = clock.millis();
        log.trace("EVAL zremrangebyscore {} 0 {} withscores limit 0 1", this, time);
        List<ByteBuffer> result = new RedisScriptCommands(redis).eval("zremrangebyscore", ScriptOutputType.MULTI,
                new ByteBuffer[]{
                        zsetKey.duplicate()
                },
                new ByteBuffer[]{
                        Lettuce.zero(),
                        PrimitiveCodec.LONG.encode(time),
                        Lettuce.withscores(),
                        Lettuce.limit(),
                        Lettuce.zero(),
                        Lettuce.one()
                });
        if (result.size() < 2) {
            return null;
        }
        return new RedisDelayed<>(decode(result.get(0)), PrimitiveCodec.LONG.decode(result.get(1)));
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
        if (log.isTraceEnabled()) {
            log.trace("ZRANGE {} 0 0 WITHSCORES", toString());
        }
        return zrangeWithScores(0, 0)
                .findFirst()
                .filter(v -> v.getScore() < clock.millis())
                .map(this::toDelayed)
                .orElse(null);
    }

    protected void await(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(zsetKey);
    }
}
