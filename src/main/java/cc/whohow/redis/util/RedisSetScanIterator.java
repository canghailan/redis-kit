package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.lettuce.ScanOutput;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class RedisSetScanIterator<T> extends RedisScanIterator<T> {
    protected final Function<ByteBuffer, T> decoder;
    protected final ByteSequence setKey;

    public RedisSetScanIterator(Redis redis, Function<ByteBuffer, T> decoder, ByteSequence key) {
        super(redis);
        this.decoder = decoder;
        this.setKey = key;
    }

    public RedisSetScanIterator(Redis redis, Function<ByteBuffer, T> decoder, ByteSequence key, String pattern, int count) {
        this(redis, decoder, key, ByteSequence.utf8(pattern), count);
    }

    public RedisSetScanIterator(Redis redis, Function<ByteBuffer, T> decoder, ByteSequence key, ByteSequence pattern, int count) {
        super(redis, pattern, count);
        this.decoder = decoder;
        this.setKey = key;
    }

    @Override
    protected RedisScanIteration<T> scan(ByteSequence cursor) {
        return redis.send(new ScanOutput<>(decoder), CommandType.SSCAN, setKey, cursor);
    }

    @Override
    protected RedisScanIteration<T> scan(ByteSequence cursor, ByteSequence pattern) {
        return redis.send(new ScanOutput<>(decoder), CommandType.SSCAN, setKey, cursor, RESP.b(CommandKeyword.MATCH), pattern);
    }

    @Override
    protected RedisScanIteration<T> scan(ByteSequence cursor, int count) {
        return redis.send(new ScanOutput<>(decoder), CommandType.SSCAN, setKey, cursor, RESP.b(CommandKeyword.COUNT), RESP.b(count));
    }

    @Override
    protected RedisScanIteration<T> scan(ByteSequence cursor, ByteSequence pattern, int count) {
        return redis.send(new ScanOutput<>(decoder), CommandType.SSCAN, setKey, cursor, RESP.b(CommandKeyword.MATCH), pattern, RESP.b(CommandKeyword.COUNT), RESP.b(count));
    }
}
