package cc.whohow.redis.util;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScoredValueScanCursor;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RedisSortedSetIterator extends RedisIterator<ScoredValue<ByteBuffer>, ScoredValueScanCursor<ByteBuffer>> {
    private static final Logger log = LogManager.getLogger();
    private final ByteBuffer sortedSetKey;

    public RedisSortedSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key) {
        super(redis);
        this.sortedSetKey = key;
    }

    public RedisSortedSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ByteBuffer match) {
        super(redis, match);
        this.sortedSetKey = key;
    }

    public RedisSortedSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, String match) {
        super(redis, match);
        this.sortedSetKey = key;
    }

    public RedisSortedSetIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ScanArgs scanArgs) {
        super(redis, scanArgs);
        this.sortedSetKey = key;
    }

    @Override
    protected ScoredValueScanCursor<ByteBuffer> scan(ScanCursor scanCursor) {
        log.trace("ZSCAN");
        return redis.zscan(sortedSetKey.duplicate(), scanCursor, scanArgs);
    }

    @Override
    protected Iterator<ScoredValue<ByteBuffer>> iterator(ScoredValueScanCursor<ByteBuffer> scanCursor) {
        return scanCursor.getValues().iterator();
    }
}
