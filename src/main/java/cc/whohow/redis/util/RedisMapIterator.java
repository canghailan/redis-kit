package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import io.lettuce.core.MapScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public class RedisMapIterator extends RedisIterator<Map.Entry<ByteBuffer, ByteBuffer>, MapScanCursor<ByteBuffer, ByteBuffer>> {
    private static final Logger log = LogManager.getLogger();
    protected final ByteBuffer hashKey;

    public RedisMapIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key) {
        super(redis);
        this.hashKey = key;
    }

    public RedisMapIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ByteBuffer match) {
        super(redis, match);
        this.hashKey = key;
    }

    public RedisMapIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, String match) {
        super(redis, match);
        this.hashKey = key;
    }

    public RedisMapIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ScanArgs scanArgs) {
        super(redis, scanArgs);
        this.hashKey = key;
    }

    @Override
    protected MapScanCursor<ByteBuffer, ByteBuffer> scan(ScanCursor scanCursor) {
        log.trace("HSCAN");
        return redis.hscan(hashKey.duplicate(), scanCursor, scanArgs);
    }

    @Override
    protected Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator(MapScanCursor<ByteBuffer, ByteBuffer> scanCursor) {
        return scanCursor.getMap().entrySet().iterator();
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(hashKey);
    }
}
