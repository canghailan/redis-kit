package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

public abstract class RedisIterator<T, C extends ScanCursor> implements Iterator<T> {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final ScanArgs scanArgs;
    protected ScanCursor scanCursor;
    protected Iterator<T> iterator;
    protected T next;

    public RedisIterator(RedisCommands<ByteBuffer, ByteBuffer> redis) {
        this(redis, new ScanArgs().limit(1000));
    }

    public RedisIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer match) {
        this(redis, ByteBuffers.toUtf8String(match));
    }

    public RedisIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, String match) {
        this(redis, new ScanArgs().match(match).limit(1000));
    }

    public RedisIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ScanArgs scanArgs) {
        this.redis = redis;
        this.scanArgs = scanArgs;
        this.scanCursor = ScanCursor.INITIAL;
        this.iterator = Collections.emptyIterator();
        this.next = null;
    }

    protected abstract C scan(ScanCursor scanCursor);

    protected abstract Iterator<T> iterator(C scanCursor);

    protected void remove(T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
        if (iterator.hasNext()) {
            return true;
        }
        while (!scanCursor.isFinished()) {
            C c = scan(scanCursor);
            scanCursor = c;
            iterator = iterator(c);
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

    @Override
    public void remove() {
        remove(next);
    }
}
