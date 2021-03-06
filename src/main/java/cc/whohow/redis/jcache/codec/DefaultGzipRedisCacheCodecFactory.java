package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.codec.Codec;
import cc.whohow.redis.codec.GzipCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

public class DefaultGzipRedisCacheCodecFactory extends DefaultRedisCacheCodecFactory {
    @Override
    public <K, V> Codec<V> newValueCodec(RedisCacheConfiguration<K, V> configuration) {
        return new GzipCodec<>(super.newValueCodec(configuration));
    }
}
