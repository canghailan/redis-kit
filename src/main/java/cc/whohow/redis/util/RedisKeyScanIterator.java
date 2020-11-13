package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.lettuce.ScanOutput;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class RedisKeyScanIterator<T> extends RedisScanIterator<T> {
    protected final Function<ByteBuffer, T> decoder;

    public RedisKeyScanIterator(Redis redis, Function<ByteBuffer, T> decoder) {
        super(redis);
        this.decoder = decoder;
    }

    public RedisKeyScanIterator(Redis redis, Function<ByteBuffer, T> decoder, String pattern, int count) {
        this(redis, decoder, ByteSequence.utf8(pattern), count);
    }

    public RedisKeyScanIterator(Redis redis, Function<ByteBuffer, T> decoder, ByteSequence pattern, int count) {
        super(redis, pattern, count);
        this.decoder = decoder;
    }

    @Override
    protected RedisScanIteration<T> scan(ByteSequence cursor) {
        return redis.send(new ScanOutput<>(decoder), CommandType.SCAN, cursor);
    }

    @Override
    protected RedisScanIteration<T> scan(ByteSequence cursor, ByteSequence pattern) {
        return redis.send(new ScanOutput<>(decoder), CommandType.SCAN, cursor, RESP.b(CommandKeyword.MATCH), pattern);
    }

    @Override
    protected RedisScanIteration<T> scan(ByteSequence cursor, int count) {
        return redis.send(new ScanOutput<>(decoder), CommandType.SCAN, cursor, RESP.b(CommandKeyword.COUNT), RESP.b(count));
    }

    @Override
    protected RedisScanIteration<T> scan(ByteSequence cursor, ByteSequence pattern, int count) {
        return redis.send(new ScanOutput<>(decoder), CommandType.SCAN, cursor, RESP.b(CommandKeyword.MATCH), pattern, RESP.b(CommandKeyword.COUNT), RESP.b(count));
    }
}
