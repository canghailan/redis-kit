package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.util.function.Function;

public interface KeyValueCodec<K, V> {
    K decodeKey(ByteBuffer bytes);

    V decodeValue(ByteBuffer bytes);

    default <T> T decodeKey(ByteBuffer bytes, Function<K, T> mapper) {
        if (bytes == null) {
            return null;
        }
        return mapper.apply(decodeKey(bytes));
    }

    default <T> T decodeValue(ByteBuffer bytes, Function<V, T> mapper) {
        if (bytes == null) {
            return null;
        }
        return mapper.apply(decodeValue(bytes));
    }

    ByteBuffer encodeKey(K key);

    ByteBuffer encodeValue(V value);
}
