package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.buffer.ByteSequence;

import java.util.Iterator;

public abstract class RedisScanIterator<T> implements Iterator<RedisScanIteration<T>> {
    protected final Redis redis;
    protected final ByteSequence pattern;
    protected final int count;
    protected boolean hasNext = true;
    protected String cursor = "0";

    public RedisScanIterator(Redis redis) {
        this(redis, null, 0);
    }

    public RedisScanIterator(Redis redis, ByteSequence pattern, int count) {
        this.redis = redis;
        this.pattern = pattern;
        this.count = count;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public RedisScanIteration<T> next() {
        RedisScanIteration<T> scanIteration = scan(cursor, pattern, count);
        hasNext = !scanIteration.isTerminate();
        return scanIteration;
    }

    protected abstract RedisScanIteration<T> scan(ByteSequence cursor);

    protected abstract RedisScanIteration<T> scan(ByteSequence cursor, ByteSequence pattern);

    protected abstract RedisScanIteration<T> scan(ByteSequence cursor, int count);

    protected abstract RedisScanIteration<T> scan(ByteSequence cursor, ByteSequence pattern, int count);

    protected RedisScanIteration<T> scan(String cursor, ByteSequence pattern, int count) {
        if (pattern != null) {
            if (count > 0) {
                return scan(ByteSequence.ascii(cursor), pattern, count);
            } else {
                return scan(ByteSequence.ascii(cursor), pattern);
            }
        } else {
            if (count > 0) {
                return scan(ByteSequence.ascii(cursor), count);
            } else {
                return scan(ByteSequence.ascii(cursor));
            }
        }
    }
}
