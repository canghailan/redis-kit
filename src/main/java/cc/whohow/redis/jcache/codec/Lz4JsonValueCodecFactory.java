package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.JacksonCodec;
import cc.whohow.redis.io.Lz4Codec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

import java.util.function.Function;

public class Lz4JsonValueCodecFactory
        implements Function<RedisCacheConfiguration, Codec> {
    @Override
    public Codec apply(RedisCacheConfiguration configuration) {
        return new Lz4Codec<>(new JacksonCodec<>(configuration.getValueTypeCanonicalName()));
    }
}
