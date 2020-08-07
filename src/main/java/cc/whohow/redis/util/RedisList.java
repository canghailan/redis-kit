package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.impl.ArrayType;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 链表、双向阻塞队列
 */
@SuppressWarnings("unchecked")
public class RedisList<E> implements List<E>, Deque<E>, BlockingDeque<E>, Supplier<List<E>> {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final ByteBuffer key;

    public RedisList(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisList(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
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
    public void addFirst(E e) {
        log.trace("LPUSH {} {}", this, e);
        redis.lpush(key.duplicate(), encode(e));
    }

    @Override
    public void addLast(E e) {
        log.trace("RPUSH {} {}", this, e);
        redis.rpush(key.duplicate(), encode(e));
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
        log.trace("LPOP {}", this);
        return decode(redis.lpop(key.duplicate()));
    }

    @Override
    public E pollLast() {
        log.trace("RPOP {}", this);
        return decode(redis.rpop(key.duplicate()));
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
        return get(0);
    }

    @Override
    public E peekLast() {
        return get(-1);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeFirstOccurrence(Object o) {
        log.trace("LREM {} 1 {}", this, o);
        return redis.lrem(key.duplicate(), 1, encode((E) o)) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeLastOccurrence(Object o) {
        log.trace("LREM {} -1 {}", this, o);
        return redis.lrem(key.duplicate(), -1, encode((E) o)) > 0;
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
        log.trace("LLEN {}", this);
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
        log.trace("LRANGE {} 0 -1", this);
        return redis.lrange(key.duplicate(), 0, -1).stream()
                .map(this::decode)
                .toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray(T[] a) {
        log.trace("LRANGE {} 0 -1", this);
        return redis.lrange(key.duplicate(), 0, -1).stream()
                .map(this::decode)
                .toArray(ArrayType.of(a)::newInstance);
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
        log.trace("RPUSH {} {}", this, c);
        redis.rpush(key.duplicate(), c.stream().map(this::encode).toArray(ByteBuffer[]::new));
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
        log.trace("DEL {}", this);
        redis.del(key.duplicate());
    }

    @Override
    public E get(int index) {
        log.trace("LINDEX {} {}", this, index);
        return decode(redis.lindex(key.duplicate(), index));
    }

    /**
     * @return null
     */
    @Override
    public E set(int index, E element) {
        log.trace("LSET {} {} {}", this, index, element);
        redis.lset(key.duplicate(), index, encode(element));
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
        log.trace("BLPOP {} 0", this);
        return decode(redis.blpop(0, key.duplicate()).getValue());
    }

    @Override
    public E takeLast() {
        log.trace("BRPOP {} 0", this);
        return decode(redis.brpop(0, key.duplicate()).getValue());
    }

    @Override
    public E pollFirst(long timeout, TimeUnit unit) {
        log.trace("BLPOP {} {}", this, unit.toSeconds(timeout));
        return redis.blpop(unit.toSeconds(timeout), key.duplicate()).map(this::decode).getValueOrElse(null);
    }

    @Override
    public E pollLast(long timeout, TimeUnit unit) {
        log.trace("BRPOP {} {}", this, unit.toSeconds(timeout));
        return redis.brpop(unit.toSeconds(timeout), key.duplicate()).map(this::decode).getValueOrElse(null);
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
        throw new UnsupportedOperationException();
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
    public List<E> get() {
        return getSubList(0, -1);
    }

    public List<E> getSubList(int fromIndex, int toIndex) {
        log.trace("LRANGE {} {} {}", this, fromIndex, toIndex);
        return redis.lrange(key.duplicate(), fromIndex, toIndex).stream()
                .map(this::decode)
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(key);
    }
}
