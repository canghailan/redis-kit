package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.KeyValueCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

@FunctionalInterface
public interface RedisCacheCodecFactory {
    <K, V> KeyValueCodec<K, V> getCodec(RedisCacheConfiguration<K, V> configuration);
}
