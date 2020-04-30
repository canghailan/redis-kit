package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrefixCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.lettuce.RedisCodecAdapter;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractRedisCacheCodecFactory implements RedisCacheCodecFactory {
    @Override
    public <K, V> RedisCodec<K, V> getCodec(RedisCacheConfiguration<K, V> configuration) {
        return new RedisCodecAdapter<>(
                new PrefixCodec<>(newKeyCodec(configuration), configuration.getRedisKeyPrefix()),
                newValueCodec(configuration));
    }

    protected abstract <K, V> Codec<K> newKeyCodec(RedisCacheConfiguration<K, V> configuration);

    protected abstract <K, V> Codec<V> newValueCodec(RedisCacheConfiguration<K, V> configuration);
}
