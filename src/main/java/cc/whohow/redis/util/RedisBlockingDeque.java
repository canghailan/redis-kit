package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.client.RedisPipeline;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.misc.RPromise;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 双向阻塞队列
 */
public class RedisBlockingDeque<E> extends RedisList<E> implements BlockingDeque<E> {
    public RedisBlockingDeque(Redis redis, String key, Codec codec) {
        super(redis, key, codec);
    }

    @Override
    public void putFirst(E e) {
        offerFirst(e);
    }

    @Override
    public void putLast(E e) {
        offerLast(e);
    }

    @Override
    public boolean offerFirst(E e, long timeout, TimeUnit unit) {
        return offerFirst(e);
    }

    @Override
    public boolean offerLast(E e, long timeout, TimeUnit unit) {
        return offerLast(e);
    }

    @Override
    public E takeFirst() {
        return pollFirst(0, TimeUnit.SECONDS);
    }

    @Override
    public E takeLast() {
        return pollLast(0, TimeUnit.SECONDS);
    }

    @Override
    public E pollFirst(long timeout, TimeUnit unit) {
        return redis.execute(codec, RedisCommands.BLPOP_VALUE, name, unit.toSeconds(timeout));
    }

    @Override
    public E pollLast(long timeout, TimeUnit unit) {
        return redis.execute(codec, RedisCommands.BRPOP_VALUE, name, unit.toSeconds(timeout));
    }

    @Override
    public void put(E e) {
        putLast(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) {
        return offerLast(e, timeout, unit);
    }

    @Override
    public E take() {
        return takeFirst();
    }

    @Override
    public E poll(long timeout, TimeUnit unit) {
        return pollFirst(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, 0);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RPromise<List<E>> r = pipeline.execute(codec, RedisCommands.LRANGE, name, 0, maxElements - 1);
        pipeline.execute(RedisCommands.LTRIM, name, maxElements, -1);
        pipeline.execute(RedisCommands.EXEC);
        pipeline.sync();
        List<E> list = r.getNow();
        c.addAll(list);
        return list.size();
    }
}
