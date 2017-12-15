package cc.whohow.redis.jcache.annotation;

import cc.whohow.redis.jcache.codec.GeneratedKeyJacksonCodec;
import cc.whohow.redis.jcache.util.CacheMethods;
import org.redisson.client.codec.Codec;

import java.lang.reflect.Method;
import java.util.function.Function;

public class DefaultKeyCodecFactory implements Function<Method, Codec> {
    @Override
    public Codec apply(Method method) {
        RedisCacheDefaults redisCacheDefaults = method.getAnnotation(RedisCacheDefaults.class);
        if (redisCacheDefaults == null || redisCacheDefaults.keyTypeCanonicalName().length == 0) {
            return new GeneratedKeyJacksonCodec(CacheMethods.getKeyTypeCanonicalName(method));
        } else {
            return new GeneratedKeyJacksonCodec(redisCacheDefaults.keyTypeCanonicalName());
        }
    }
}
