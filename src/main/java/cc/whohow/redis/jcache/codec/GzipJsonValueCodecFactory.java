package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.GzipCodec;
import cc.whohow.redis.io.JacksonCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

import java.util.function.Function;

public class GzipJsonValueCodecFactory
        implements Function<RedisCacheConfiguration, Codec> {
    @Override
    public Codec apply(RedisCacheConfiguration configuration) {
        return new GzipCodec<>(new JacksonCodec<>(configuration.getValueTypeCanonicalName()));
    }
}
