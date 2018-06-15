package cc.whohow.redis.io;

import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class KeyValueCodecAdapter<K, V> implements KeyValueCodec<K, V>, RedisCodec<K, V> {
    private final KeyValueCodec<K, V> keyValueCodec;

    public KeyValueCodecAdapter(KeyValueCodec<K, V> keyValueCodec) {
        this.keyValueCodec = keyValueCodec;
    }

    @Override
    public K decodeKey(ByteBuffer bytes) {
        return keyValueCodec.decodeKey(bytes);
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return keyValueCodec.decodeValue(bytes);
    }

    @Override
    public ByteBuffer encodeKey(K key) {
        return keyValueCodec.encodeKey(key);
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return keyValueCodec.encodeValue(value);
    }

    @Override
    public <T> T decodeKey(ByteBuffer bytes, Function<K, T> mapper) {
        return keyValueCodec.decodeKey(bytes, mapper);
    }

    @Override
    public <T> T decodeValue(ByteBuffer bytes, Function<V, T> mapper) {
        return keyValueCodec.decodeValue(bytes, mapper);
    }
}
