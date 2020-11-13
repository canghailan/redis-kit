package cc.whohow.redis.util;

import java.util.Collections;
import java.util.Iterator;

public class RedisIterator<T> implements Iterator<T> {
    protected final Iterator<RedisScanIteration<T>> scanIterator;
    protected Iterator<T> iterator;
    protected T next;

    public RedisIterator(Iterator<RedisScanIteration<T>> scanIterator) {
        this.scanIterator = scanIterator;
        this.iterator = Collections.emptyIterator();
        this.next = null;
    }

    @Override
    public boolean hasNext() {
        if (iterator.hasNext()) {
            return true;
        }
        while (scanIterator.hasNext()) {
            iterator = scanIterator.next().getArray().iterator();
            if (iterator.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public T next() {
        return next = iterator.next();
    }
}
