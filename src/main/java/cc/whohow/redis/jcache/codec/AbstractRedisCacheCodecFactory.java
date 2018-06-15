package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.KeyValueCodec;
import cc.whohow.redis.io.KeyValueCodecFacade;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

public abstract class AbstractRedisCacheCodecFactory implements RedisCacheCodecFactory {
    @Override
    public <K, V> KeyValueCodec<K, V> getCodec(RedisCacheConfiguration<K, V> configuration) {
        return new KeyValueCodecFacade<>(
                new RedisCacheKeyCodec<>(configuration.getName(), getSeparator(configuration), newKeyCodec(configuration)),
                new RedisCacheValueCodec<>(newValueCodec(configuration)));
    }

    protected <K, V> String getSeparator(RedisCacheConfiguration<K, V> configuration) {
        return configuration.getKeyTypeCanonicalName().length == 0 ? "" : ":";
    }

    protected abstract <K, V> Codec<K> newKeyCodec(RedisCacheConfiguration<K, V> configuration);

    protected abstract <K, V> Codec<V> newValueCodec(RedisCacheConfiguration<K, V> configuration);
}
