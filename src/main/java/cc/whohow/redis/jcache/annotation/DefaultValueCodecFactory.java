package cc.whohow.redis.jcache.annotation;

import cc.whohow.redis.codec.ObjectJacksonCodec;
import cc.whohow.redis.jcache.util.CacheMethods;
import org.redisson.client.codec.Codec;

import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.function.Function;

public class DefaultValueCodecFactory implements Function<Method, Codec> {
    @Override
    public Codec apply(Method method) {
        RedisCacheDefaults redisCacheDefaults = method.getAnnotation(RedisCacheDefaults.class);
        if (redisCacheDefaults == null) {
            return new ObjectJacksonCodec(CacheMethods.getValueTypeCanonicalName(method));
        } else {
            String valueTypeCanonicalName = redisCacheDefaults.valueTypeCanonicalName();
            if (valueTypeCanonicalName.isEmpty()) {
                valueTypeCanonicalName = CacheMethods.getValueTypeCanonicalName(method);
            }

            Codec codec = new ObjectJacksonCodec(valueTypeCanonicalName);
            if (redisCacheDefaults.valueCompressionCodec().isEmpty()) {
                return codec;
            }

            try {
                return (Codec) Class.forName(redisCacheDefaults.valueCompressionCodec())
                        .getConstructor(Codec.class)
                        .newInstance(codec);
            } catch (Exception e) {
                throw new UndeclaredThrowableException(e);
            }
        }
    }
}
