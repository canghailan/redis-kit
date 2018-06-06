package cc.whohow.redis.codec;

import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.util.Optional;

public interface OptionalRedisCodec<K, V> extends RedisCodec<K, V> {
    Optional<V> decodeOptionalValue(ByteBuffer bytes);
}
