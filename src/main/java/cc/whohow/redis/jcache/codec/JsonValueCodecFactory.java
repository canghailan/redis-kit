package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.JacksonCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

import java.util.function.Function;

public class JsonValueCodecFactory
        implements Function<RedisCacheConfiguration, Codec> {
    @Override
    public Codec apply(RedisCacheConfiguration configuration) {
        return new JacksonCodec<>(configuration.getValueTypeCanonicalName());
    }
}
