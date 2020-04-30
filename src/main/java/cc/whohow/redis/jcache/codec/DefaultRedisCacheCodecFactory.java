package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.JacksonCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

public class DefaultRedisCacheCodecFactory extends AbstractRedisCacheCodecFactory {
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected <K, V> Codec<K> newKeyCodec(RedisCacheConfiguration<K, V> configuration) {
        return (Codec) ImmutableGeneratedCacheKeyCodec.create(configuration.getKeyTypeCanonicalName());
    }

    @Override
    protected <K, V> Codec<V> newValueCodec(RedisCacheConfiguration<K, V> configuration) {
        return new JacksonCodec<>(configuration.getValueTypeCanonicalName());
    }
}
