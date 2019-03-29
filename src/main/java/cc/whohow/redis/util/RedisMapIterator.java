package cc.whohow.redis.util;

import io.lettuce.core.MapScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public class RedisMapIterator extends RedisIterator<Map.Entry<ByteBuffer, ByteBuffer>, MapScanCursor<ByteBuffer, ByteBuffer>> {
    private ByteBuffer key;

    public RedisMapIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key) {
        super(redis);
        this.key = key;
    }

    public RedisMapIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ByteBuffer match) {
        super(redis, match);
        this.key = key;
    }

    public RedisMapIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, String match) {
        super(redis, match);
        this.key = key;
    }

    public RedisMapIterator(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, ScanArgs scanArgs) {
        super(redis, scanArgs);
        this.key = key;
    }

    @Override
    protected MapScanCursor<ByteBuffer, ByteBuffer> scan(ScanCursor scanCursor) {
        return redis.hscan(key.duplicate(), scanCursor, scanArgs);
    }

    @Override
    protected Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator(MapScanCursor<ByteBuffer, ByteBuffer> scanCursor) {
        return scanCursor.getMap().entrySet().iterator();
    }

    @Override
    protected void remove(Map.Entry<ByteBuffer, ByteBuffer> value) {
        redis.hdel(key.duplicate(), value.getKey().duplicate());
    }
}
