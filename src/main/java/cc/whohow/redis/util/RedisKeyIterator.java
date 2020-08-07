package cc.whohow.redis.util;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RedisKeyIterator extends RedisIterator<ByteBuffer, KeyScanCursor<ByteBuffer>> {
    private static final Logger log = LogManager.getLogger();

    public RedisKeyIterator(RedisCommands<ByteBuffer, ByteBuffer> redis) {
        super(redis);
    }

    public RedisKeyIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer match) {
        super(redis, match);
    }

    public RedisKeyIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, String match) {
        super(redis, match);
    }

    public RedisKeyIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ScanArgs scanArgs) {
        super(redis, scanArgs);
    }

    @Override
    protected KeyScanCursor<ByteBuffer> scan(ScanCursor scanCursor) {
        log.trace("SCAN {} [match?] [limit?]", scanCursor.getCursor());
        return redis.scan(scanCursor, scanArgs);
    }

    @Override
    protected Iterator<ByteBuffer> iterator(KeyScanCursor<ByteBuffer> scanCursor) {
        return scanCursor.getKeys().iterator();
    }

    @Override
    protected void remove(ByteBuffer value) {
        log.trace("DEL [value?]");
        redis.del(value.duplicate());
    }
}
