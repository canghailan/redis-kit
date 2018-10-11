package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import io.lettuce.core.codec.RedisCodec;

@FunctionalInterface
public interface RedisCacheCodecFactory {
    <K, V> RedisCodec<K, V> getCodec(RedisCacheConfiguration<K, V> configuration);
}
