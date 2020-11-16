package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.Codec;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 链表、双向阻塞队列
 */
public class RedisList<E>
        extends AbstractRedisList<E>
        implements List<E>, Deque<E>, BlockingDeque<E>, Copyable<List<E>> {
    public RedisList(Redis redis, Codec<E> codec, String key) {
        this(redis, codec, ByteSequence.utf8(key));
    }

    public RedisList(Redis redis, Codec<E> codec, ByteSequence key) {
        super(redis, codec, key);
    }

    @Override
    public void addFirst(E e) {
        lpush(e);
    }

    @Override
    public void addLast(E e) {
        rpush(e);
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
        return lpop();
    }

    @Override
    public E pollLast() {
        return rpop();
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
        return lindex(0);
    }

    @Override
    public E peekLast() {
        return lindex(-1);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeFirstOccurrence(Object o) {
        return lrem(1, (E) o) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeLastOccurrence(Object o) {
        return lrem(-1, (E) o) > 0;
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
        return llen().intValue();
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
        return lrange(0, -1)
                .toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray(T[] a) {
        return lrange(0, -1)
                .toArray(a);
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
        return rpush(c) > 0;
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
        del();
    }

    @Override
    public E get(int index) {
        return lindex(index);
    }

    /**
     * @return null
     */
    @Override
    public E set(int index, E element) {
        lset(index, element);
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
        return blpop(0);
    }

    @Override
    public E takeLast() {
        return brpop(0);
    }

    @Override
    public E pollFirst(long timeout, TimeUnit unit) {
        return blpop(unit.toSeconds(timeout));
    }

    @Override
    public E pollLast(long timeout, TimeUnit unit) {
        return brpop(unit.toSeconds(timeout));
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
        return drainTo(c, 1024);
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

    @Override
    public List<E> copy() {
        return copy(0, -1);
    }

    public List<E> copy(int fromIndex, int toIndex) {
        return lrange(fromIndex, toIndex);
    }
}
