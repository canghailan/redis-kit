package cc.whohow.redis.util;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ValueScanCursor;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RedisSetIterator extends RedisIterator<ByteBuffer, ValueScanCursor<ByteBuffer>> {
    private final ByteBuffer key;

    public RedisSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key) {
        super(redis);
        this.key = key;
    }

    public RedisSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ByteBuffer match) {
        super(redis, match);
        this.key = key;
    }

    public RedisSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, String match) {
        super(redis, match);
        this.key = key;
    }

    public RedisSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ScanArgs scanArgs) {
        super(redis, scanArgs);
        this.key = key;
    }

    @Override
    protected ValueScanCursor<ByteBuffer> scan(ScanCursor scanCursor) {
        return redis.sscan(key.duplicate(), scanCursor, scanArgs);
    }

    @Override
    protected Iterator<ByteBuffer> iterator(ValueScanCursor<ByteBuffer> scanCursor) {
        return scanCursor.getValues().iterator();
    }

    @Override
    protected void remove(ByteBuffer value) {
        redis.srem(key.duplicate(), value.duplicate());
    }
}
