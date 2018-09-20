package cc.whohow.redis.io;

import java.nio.ByteBuffer;

/**
 * 键值对编码器
 */
public interface KeyValueCodec<K, V> {
    K decodeKey(ByteBuffer bytes);

    V decodeValue(ByteBuffer bytes);

    ByteBuffer encodeKey(K key);

    ByteBuffer encodeValue(V value);
}
