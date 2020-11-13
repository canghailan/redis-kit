package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.lettuce.ScanHashOutput;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class RedisHashScanIterator<K, V> extends RedisScanIterator<Map.Entry<K, V>> {
    protected final Function<ByteBuffer, K> keyDecoder;
    protected final Function<ByteBuffer, V> valueDecoder;
    protected final ByteSequence hashKey;

    public RedisHashScanIterator(Redis redis, Function<ByteBuffer, K> keyDecoder, Function<ByteBuffer, V> valueDecoder,
                                 ByteSequence key) {
        super(redis);
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.hashKey = key;
    }

    public RedisHashScanIterator(Redis redis, Function<ByteBuffer, K> keyDecoder, Function<ByteBuffer, V> valueDecoder,
                                 ByteSequence key, String pattern, int count) {
        this(redis, keyDecoder, valueDecoder, key, ByteSequence.utf8(pattern), count);
    }

    public RedisHashScanIterator(Redis redis, Function<ByteBuffer, K> keyDecoder, Function<ByteBuffer, V> valueDecoder,
                                 ByteSequence key, ByteSequence pattern, int count) {
        super(redis, pattern, count);
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.hashKey = key;
    }

    @Override
    protected RedisScanIteration<Map.Entry<K, V>> scan(ByteSequence cursor) {
        return redis.send(new ScanHashOutput<>(keyDecoder, valueDecoder), CommandType.HSCAN, hashKey, cursor);
    }

    @Override
    protected RedisScanIteration<Map.Entry<K, V>> scan(ByteSequence cursor, ByteSequence pattern) {
        return redis.send(new ScanHashOutput<>(keyDecoder, valueDecoder), CommandType.HSCAN, hashKey, cursor, RESP.b(CommandKeyword.MATCH), pattern);
    }

    @Override
    protected RedisScanIteration<Map.Entry<K, V>> scan(ByteSequence cursor, int count) {
        return redis.send(new ScanHashOutput<>(keyDecoder, valueDecoder), CommandType.HSCAN, hashKey, cursor, RESP.b(CommandKeyword.COUNT), RESP.b(count));
    }

    @Override
    protected RedisScanIteration<Map.Entry<K, V>> scan(ByteSequence cursor, ByteSequence pattern, int count) {
        return redis.send(new ScanHashOutput<>(keyDecoder, valueDecoder), CommandType.HSCAN, hashKey, cursor, RESP.b(CommandKeyword.MATCH), pattern, RESP.b(CommandKeyword.COUNT), RESP.b(count));
    }
}
