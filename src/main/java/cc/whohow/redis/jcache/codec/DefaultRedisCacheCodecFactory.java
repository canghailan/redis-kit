package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.codec.Codec;
import cc.whohow.redis.codec.JacksonCodec;
import cc.whohow.redis.codec.PrefixCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

public class DefaultRedisCacheCodecFactory implements RedisCacheCodecFactory {
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <K, V> Codec<K> newKeyCodec(RedisCacheConfiguration<K, V> configuration) {
        return new PrefixCodec<K>((Codec) ImmutableGeneratedCacheKeyCodec.create(configuration.getKeyTypeCanonicalName()), configuration.getRedisKeyPrefix());
    }

    @Override
    public <K, V> Codec<V> newValueCodec(RedisCacheConfiguration<K, V> configuration) {
        return new JacksonCodec<>(configuration.getValueTypeCanonicalName());
    }
}
