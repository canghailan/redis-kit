package cc.whohow.redis.jcache.codec;

import java.nio.ByteBuffer;
import java.util.Optional;

public class RedisCacheCodecAdapter<K, V> implements RedisCacheCodec<K, V>{
    private final String cacheName;
    private final
    private final ByteBuffer keyPrefix;

    @Override
    public ByteBuffer getKeyPrefix() {
        return null;
    }

    @Override
    public Optional<V> decodeOptionalValue(ByteBuffer bytes) {
        return Optional.empty();
    }

    @Override
    public K decodeKey(ByteBuffer bytes) {
        return null;
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return null;
    }

    @Override
    public ByteBuffer encodeKey(K key) {
        return null;
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return null;
    }
}
