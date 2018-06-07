package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisPipeline;
import cc.whohow.redis.codec.Codecs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.redisson.api.RFuture;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 集合、缓冲队列
 */
public class RedisSet<E> implements Set<E>, Queue<E> {
    public static final RedisCommand<Integer> SREM = new RedisCommand<>("SREM", new IntegerReplayConvertor());

    protected final Redis redis;
    protected final ByteBuf name;
    protected final Codec codec;

    public RedisSet(Redis redis, String name, Codec codec) {
        this.redis = redis;
        this.name = Unpooled.copiedBuffer(name, StandardCharsets.UTF_8).asReadOnly();
        this.codec = codec;
    }

    @Override
    public int size() {
        return redis.execute(RedisCommands.SCARD_INT, name.retain());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return redis.execute(RedisCommands.SISMEMBER, name.retain(), Codecs.encode(codec, o));
    }

    @Override
    @Deprecated
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        Set<E> set = redis.execute(codec, RedisCommands.SMEMBERS, name.retain());
        return set.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        Set<T> set = redis.execute(codec, RedisCommands.SMEMBERS, name.retain());
        return set.toArray(a);
    }

    @Override
    public boolean add(E e) {
        return redis.execute(RedisCommands.SADD, name.retain(), Codecs.encode(codec, e)) > 0;
    }

    @Override
    public boolean remove(Object o) {
        return redis.execute(SREM, name.retain(), Codecs.encode(codec, o)) > 0;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        List<RFuture<Boolean>> list = new ArrayList<>(c.size());
        ByteBuf[] elements = Codecs.encode(codec, c);
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        for (ByteBuf e : elements) {
            list.add(pipeline.execute(RedisCommands.SISMEMBER, name.retain(), e));
        }
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return list.stream().allMatch(RFuture::getNow);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return redis.execute(RedisCommands.SADD, (Object[]) Codecs.concat(name.retain(), Codecs.encode(codec, c))) > 0;
    }

    @Override
    @Deprecated
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return redis.execute(SREM, (Object[]) Codecs.concat(name.retain(), Codecs.encode(codec, c))) > 0;
    }

    @Override
    public void clear() {
        redis.execute(RedisCommands.DEL, name.retain());
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
        return redis.execute(codec, RedisCommands.SPOP_SINGLE, name.retain());
    }

    @Override
    public E element() {
        return checkElement(peek());
    }

    @Override
    public E peek() {
        return redis.execute(codec, RedisCommands.SRANDMEMBER_SINGLE, name.retain());
    }

    protected E checkElement(E e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public String toString() {
        return "RedisSet{" +
                "redisClient=" + redis +
                ", name=" + name.toString(StandardCharsets.UTF_8) +
                ", codec=" + codec +
                '}';
    }
}
