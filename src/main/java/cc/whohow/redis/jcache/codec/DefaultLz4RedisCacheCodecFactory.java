package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.Lz4Codec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

public class DefaultLz4RedisCacheCodecFactory extends DefaultRedisCacheCodecFactory {
    @Override
    protected <K, V> Codec<V> newValueCodec(RedisCacheConfiguration<K, V> configuration) {
        return new Lz4Codec<>(super.newValueCodec(configuration));
    }
}
