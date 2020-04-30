package cc.whohow.redis.util;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScoredValueScanCursor;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RedisSortedSetIterator extends RedisIterator<ScoredValue<ByteBuffer>, ScoredValueScanCursor<ByteBuffer>> {
    private final ByteBuffer key;

    public RedisSortedSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key) {
        super(redis);
        this.key = key;
    }

    public RedisSortedSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ByteBuffer match) {
        super(redis, match);
        this.key = key;
    }

    public RedisSortedSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, String match) {
        super(redis, match);
        this.key = key;
    }

    public RedisSortedSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ScanArgs scanArgs) {
        super(redis, scanArgs);
        this.key = key;
    }

    @Override
    protected ScoredValueScanCursor<ByteBuffer> scan(ScanCursor scanCursor) {
        return redis.zscan(key.duplicate(), scanCursor, scanArgs);
    }

    @Override
    protected Iterator<ScoredValue<ByteBuffer>> iterator(ScoredValueScanCursor<ByteBuffer> scanCursor) {
        return scanCursor.getValues().iterator();
    }

    @Override
    protected void remove(ScoredValue<ByteBuffer> value) {
        redis.srem(key.duplicate(), value.getValue().duplicate());
    }
}
