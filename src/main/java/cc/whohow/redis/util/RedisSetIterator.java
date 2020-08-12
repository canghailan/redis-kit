package cc.whohow.redis.util;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ValueScanCursor;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RedisSetIterator extends RedisIterator<ByteBuffer, ValueScanCursor<ByteBuffer>> {
    private static final Logger log = LogManager.getLogger();
    protected final ByteBuffer setKey;

    public RedisSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key) {
        super(redis);
        this.setKey = key;
    }

    public RedisSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ByteBuffer match) {
        super(redis, match);
        this.setKey = key;
    }

    public RedisSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, String match) {
        super(redis, match);
        this.setKey = key;
    }

    public RedisSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ScanArgs scanArgs) {
        super(redis, scanArgs);
        this.setKey = key;
    }

    @Override
    protected ValueScanCursor<ByteBuffer> scan(ScanCursor scanCursor) {
        log.trace("SSCAN");
        return redis.sscan(setKey.duplicate(), scanCursor, scanArgs);
    }

    @Override
    protected Iterator<ByteBuffer> iterator(ValueScanCursor<ByteBuffer> scanCursor) {
        return scanCursor.getValues().iterator();
    }
}
