package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.RedisCommandsAdapter;
import cc.whohow.redis.lettuce.RedisValueCodecAdapter;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reids延迟队列
 */
public class RedisDelayQueue<E> implements BlockingQueue<RedisDelayed<E>> {
    private static final ByteBuffer ZERO = PrimitiveCodec.INTEGER.encode(0);
    private static final ByteBuffer ONE = PrimitiveCodec.INTEGER.encode(1);
    private static final ByteBuffer WITHSCORES = ByteBuffers.fromUtf8("WITHSCORES");
    private static final ByteBuffer LIMIT = ByteBuffers.fromUtf8("LIMIT");

    protected final AtomicLong pollInterval = new AtomicLong(1000);
    protected final RedisCommands<ByteBuffer, E> redis;
    protected final RedisScriptCommands redisScriptCommands;
    protected final ByteBuffer key;
    protected final Codec<E> codec;
    protected final Clock clock;

    public RedisDelayQueue(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key, Clock clock) {
        this.redis = new RedisCommandsAdapter<>(redis, new RedisValueCodecAdapter<>(codec));
        this.redisScriptCommands = new RedisScriptCommands(redis);
        this.key = ByteBuffers.fromUtf8(key);
        this.codec = codec;
        this.clock = clock;
    }

    public RedisDelayQueue(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, key, new RedisClock(redis));
    }

    public Clock getClock() {
        return clock;
    }

    public Duration getPollInterval() {
        return Duration.ofMillis(this.pollInterval.get());
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval.set(pollInterval.toMillis());
    }

    public void setPollInterval(long pollInterval, TimeUnit unit) {
        this.pollInterval.set(unit.toMillis(pollInterval));
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
        return redis.zscore(this.key.duplicate(), (E) key) != null;
    }

    @Override
    public int drainTo(Collection<? super RedisDelayed<E>> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super RedisDelayed<E>> c, int maxElements) {
        long time = clock.millis();
        List<ByteBuffer> result = redisScriptCommands.eval("zremrangebyscore", ScriptOutputType.MULTI,
                new ByteBuffer[]{key.duplicate()},
                new ByteBuffer[]{
                        ZERO.duplicate(),
                        PrimitiveCodec.LONG.encode(time),
                        WITHSCORES.duplicate(),
                        LIMIT.duplicate(),
                        ZERO.duplicate(),
                        PrimitiveCodec.INTEGER.encode(maxElements)
                });
        for (int i = 0; i < result.size(); i += 2) {
            c.add(new RedisDelayed<>(codec.decode(result.get(i)), PrimitiveCodec.LONG.decode(result.get(i + 1))));
        }
        return result.size() / 2;
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Iterator<RedisDelayed<E>> iterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(RedisDelayed<E> RedisDelayed) {
        return redis.zadd(this.key.duplicate(), RedisDelayed.getDelay(TimeUnit.MILLISECONDS), RedisDelayed.get()) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
        RedisDelayed<E> RedisDelayed = (RedisDelayed<E>) o;
        return redis.zrem(this.key.duplicate(), RedisDelayed.get()) > 0;
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
    public boolean addAll(Collection<? extends RedisDelayed<E>> c) {
        ScoredValue[] encodedScoredValues = c.stream()
                .map(e -> ScoredValue.fromNullable(e.getDelay(TimeUnit.MILLISECONDS), e.get()))
                .toArray(ScoredValue[]::new);
        return redis.zadd(key.duplicate(), encodedScoredValues) > 0;
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
        redis.del(key.duplicate());
    }

    @Override
    public boolean offer(RedisDelayed<E> RedisDelayed) {
        return add(RedisDelayed);
    }

    @Override
    public void put(RedisDelayed<E> RedisDelayed) throws InterruptedException {
        add(RedisDelayed);
    }

    @Override
    public boolean offer(RedisDelayed<E> RedisDelayed, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(RedisDelayed);
    }

    @Override
    public RedisDelayed<E> take() throws InterruptedException {
        while (true) {
            RedisDelayed<E> value = poll();
            if (value != null) {
                return value;
            }
            Thread.sleep(pollInterval.get());
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
            Thread.sleep(pollInterval.get());
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
        List<ByteBuffer> result = redisScriptCommands.eval("zremrangebyscore", ScriptOutputType.MULTI,
                new ByteBuffer[]{key.duplicate()},
                ZERO.duplicate(),
                PrimitiveCodec.LONG.encode(time),
                WITHSCORES.duplicate(),
                LIMIT.duplicate(),
                ZERO.duplicate(),
                ONE.duplicate());
        if (result.size() < 2) {
            return null;
        }
        return new RedisDelayed<>(codec.decode(result.get(0)), PrimitiveCodec.LONG.decode(result.get(1)));
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
        return redis.zrangeWithScores(this.key.duplicate(), 0, 0).stream()
                .findFirst()
                .filter(v -> v.getScore() < clock.millis())
                .map(v -> new RedisDelayed<>(v.getValue(), (long) v.getScore()))
                .orElse(null);
    }
}
