package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisPipeline;
import cc.whohow.redis.codec.Codecs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.redisson.api.RFuture;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommands;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 链表、双向阻塞队列
 */
public class RedisList<E> implements List<E>, Deque<E>, BlockingDeque<E> {
    protected final Redis redis;
    protected final ByteBuf name;
    protected final Codec codec;

    public RedisList(Redis redis, String name, Codec codec) {
        this.redis = redis;
        this.name = Unpooled.copiedBuffer(name, StandardCharsets.UTF_8).asReadOnly();
        this.codec = codec;
    }

    @Override
    public void addFirst(E e) {
        redis.execute(RedisCommands.LPUSH, name.retain(), Codecs.encode(codec, e));
    }

    @Override
    public void addLast(E e) {
        redis.execute(RedisCommands.RPUSH, name.retain(), Codecs.encode(codec, e));
    }

    @Override
    public boolean offerFirst(E e) {
        addFirst(e);
        return true;
    }

    @Override
    public boolean offerLast(E e) {
        addLast(e);
        return false;
    }

    @Override
    public E removeFirst() {
        return checkElement(pollFirst());
    }

    @Override
    public E removeLast() {
        return checkElement(pollLast());
    }

    @Override
    public E pollFirst() {
        return redis.execute(codec, RedisCommands.LPOP, name.retain());
    }

    @Override
    public E pollLast() {
        return redis.execute(codec, RedisCommands.RPOP, name.retain());
    }

    @Override
    public E getFirst() {
        return checkElement(peekFirst());
    }

    @Override
    public E getLast() {
        return checkElement(peekLast());
    }

    @Override
    public E peekFirst() {
        return redis.execute(codec, RedisCommands.LINDEX, name.retain(), 0);
    }

    @Override
    public E peekLast() {
        return redis.execute(codec, RedisCommands.LINDEX, name.retain(), -1);
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
        return redis.execute(RedisCommands.LREM_SINGLE, name.retain(), 1, Codecs.encode(codec, o));
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
        return redis.execute(RedisCommands.LREM_SINGLE, name.retain(), -1, Codecs.encode(codec, o));
    }

    @Override
    public boolean offer(E e) {
        return offerLast(e);
    }

    @Override
    public E remove() {
        return removeFirst();
    }

    @Override
    public E poll() {
        return pollFirst();
    }

    @Override
    public E element() {
        return getFirst();
    }

    @Override
    public E peek() {
        return peekFirst();
    }

    @Override
    public void push(E e) {
        addFirst(e);
    }

    @Override
    public E pop() {
        return removeFirst();
    }

    @Override
    @Deprecated
    public Iterator<E> descendingIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return redis.execute(RedisCommands.LLEN_INT, name.retain());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @Deprecated
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        List<?> list = redis.execute(codec, RedisCommands.LRANGE, name.retain(), 0, -1);
        return list.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        List<T> list = redis.execute(codec, RedisCommands.LRANGE, name.retain(), 0, -1);
        return list.toArray(a);
    }

    @Override
    public boolean add(E e) {
        return offerLast(e);
    }

    @Override
    public boolean remove(Object o) {
        return removeFirstOccurrence(o);
    }

    @Override
    @Deprecated
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        redis.execute(RedisCommands.RPUSH, (Object[]) Codecs.concat(name.retain(), Codecs.encode(codec, c)));
        return true;
    }

    @Override
    @Deprecated
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        redis.execute(RedisCommands.DEL, name.retain());
    }

    @Override
    public E get(int index) {
        return redis.execute(codec, RedisCommands.LINDEX, name.retain(), index);
    }

    @Override
    public E set(int index, E element) {
        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MULTI);
        RFuture<E> r = pipeline.execute(codec, RedisCommands.LINDEX, name.retain(), index);
        pipeline.execute(RedisCommands.LSET, name.retain(), index, Codecs.encode(codec, element));
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        return r.getNow();
    }

    public void lset(int index, E element) {
        redis.execute(RedisCommands.LSET, name.retain(), index, Codecs.encode(codec, element));
    }

    @Override
    @Deprecated
    public void add(int index, E element) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public E remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public ListIterator<E> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public ListIterator<E> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public List<E> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
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
        return redis.execute(codec, RedisCommands.BLPOP_VALUE, name.retain(), unit.toSeconds(timeout));
    }

    @Override
    public E pollLast(long timeout, TimeUnit unit) {
        return redis.execute(codec, RedisCommands.BRPOP_VALUE, name.retain(), unit.toSeconds(timeout));
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
        RFuture<List<E>> r = pipeline.execute(codec, RedisCommands.LRANGE, name.retain(), 0, maxElements - 1);
        pipeline.execute(RedisCommands.LTRIM, name.retain(), maxElements, -1);
        pipeline.execute(RedisCommands.EXEC);
        pipeline.flush();
        List<E> list = r.getNow();
        c.addAll(list);
        return list.size();
    }

    protected E checkElement(E e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public String toString() {
        return "RedisList{" +
                "redis=" + redis +
                ", name=" + name.toString(StandardCharsets.UTF_8) +
                ", codec=" + codec +
                '}';
    }
}
