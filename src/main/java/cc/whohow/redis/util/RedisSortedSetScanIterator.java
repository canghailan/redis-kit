package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.lettuce.ScanSortedSetOutput;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class RedisSortedSetScanIterator<T> extends RedisScanIterator<ScoredValue<T>> {
    protected final Function<ByteBuffer, T> decoder;
    protected final ByteSequence sortedSetKey;

    public RedisSortedSetScanIterator(Redis redis, Function<ByteBuffer, T> decoder, ByteSequence key) {
        super(redis);
        this.decoder = decoder;
        this.sortedSetKey = key;
    }

    public RedisSortedSetScanIterator(Redis redis, Function<ByteBuffer, T> decoder, ByteSequence key, String pattern, int count) {
        this(redis, decoder, key, ByteSequence.utf8(pattern), count);
    }

    public RedisSortedSetScanIterator(Redis redis, Function<ByteBuffer, T> decoder, ByteSequence key, ByteSequence pattern, int count) {
        super(redis, pattern, count);
        this.decoder = decoder;
        this.sortedSetKey = key;
    }

    @Override
    protected RedisScanIteration<ScoredValue<T>> scan(ByteSequence cursor) {
        return redis.send(new ScanSortedSetOutput<>(decoder), CommandType.ZSCAN, sortedSetKey, cursor);
    }

    @Override
    protected RedisScanIteration<ScoredValue<T>> scan(ByteSequence cursor, ByteSequence pattern) {
        return redis.send(new ScanSortedSetOutput<>(decoder), CommandType.ZSCAN, sortedSetKey, cursor, RESP.b(CommandKeyword.MATCH), pattern);
    }

    @Override
    protected RedisScanIteration<ScoredValue<T>> scan(ByteSequence cursor, int count) {
        return redis.send(new ScanSortedSetOutput<>(decoder), CommandType.ZSCAN, sortedSetKey, cursor, RESP.b(CommandKeyword.COUNT), RESP.b(count));
    }

    @Override
    protected RedisScanIteration<ScoredValue<T>> scan(ByteSequence cursor, ByteSequence pattern, int count) {
        return redis.send(new ScanSortedSetOutput<>(decoder), CommandType.ZSCAN, sortedSetKey, cursor, RESP.b(CommandKeyword.MATCH), pattern, RESP.b(CommandKeyword.COUNT), RESP.b(count));
    }
}
