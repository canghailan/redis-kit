package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.codec.Codec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

public interface RedisCacheCodecFactory {
    <K, V> Codec<K> newKeyCodec(RedisCacheConfiguration<K, V> configuration);

    <K, V> Codec<V> newValueCodec(RedisCacheConfiguration<K, V> configuration);
}
