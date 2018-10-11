package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.lettuce.RedisCommandsAdapter;
import cc.whohow.redis.lettuce.RedisValueCodecAdapter;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 链表、双向阻塞队列
 */
@SuppressWarnings("unchecked")
public class RedisList<E> implements List<E>, Deque<E>, BlockingDeque<E> {
    protected final RedisCommands<ByteBuffer, E> redis;
    protected final ByteBuffer key;

    public RedisList(RedisCommands<ByteBuffer, E> redis, ByteBuffer key) {
        this.redis = redis;
        this.key = key;
    }

    public RedisList(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(new RedisCommandsAdapter<>(redis, new RedisValueCodecAdapter<>(codec)), ByteBuffers.fromUtf8(key));
    }

    @Override
    public void addFirst(E e) {
        redis.lpush(key.duplicate(), e);
    }

    @Override
    public void addLast(E e) {
        redis.rpush(key.duplicate(), e);
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
        return redis.lpop(key.duplicate());
    }

    @Override
    public E pollLast() {
        return redis.rpop(key.duplicate());
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
        return redis.lindex(key.duplicate(), 0);
    }

    @Override
    public E peekLast() {
        return redis.lindex(key.duplicate(), -1);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeFirstOccurrence(Object o) {
        return redis.lrem(key.duplicate(), 1, (E) o) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeLastOccurrence(Object o) {
        return redis.lrem(key.duplicate(), -1, (E) o) > 0;
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

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Iterator<E> descendingIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return redis.llen(key.duplicate()).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        return redis.lrange(key.duplicate(), 0, -1).toArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        return redis.lrange(key.duplicate(), 0, -1).toArray(a);
    }

    @Override
    public boolean add(E e) {
        return offerLast(e);
    }

    @Override
    public boolean remove(Object o) {
        return removeFirstOccurrence(o);
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
        redis.rpush(key.duplicate(), (E[]) c.toArray());
        return true;
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException();
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
    public E get(int index) {
        return redis.lindex(key.duplicate(), index);
    }

    /**
     * @return null
     */
    @Override
    public E set(int index, E element) {
        redis.lset(key.duplicate(), index, element);
        return null;
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public void add(int index, E element) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public E remove(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public ListIterator<E> listIterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public ListIterator<E> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
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
        return redis.blpop(0, key.duplicate()).getValue();
    }

    @Override
    public E takeLast() {
        return redis.brpop(0, key.duplicate()).getValue();
    }

    @Override
    public E pollFirst(long timeout, TimeUnit unit) {
        return redis.blpop(unit.toSeconds(timeout), key.duplicate()).getValueOrElse(null);
    }

    @Override
    public E pollLast(long timeout, TimeUnit unit) {
        return redis.brpop(unit.toSeconds(timeout), key.duplicate()).getValueOrElse(null);
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
        return drainTo(c, 1);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        E head = poll();
        if (head == null) {
            return 0;
        }
        c.add(head);
        return 1;
    }

    protected E checkElement(E e) {
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }
}
