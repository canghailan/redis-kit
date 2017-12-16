package cc.whohow.redis.jcache.annotation;

import org.redisson.client.codec.Codec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface RedisCacheDefaults {
    boolean statisticsEnabled() default true;

    boolean managementEnabled() default false;

    long expiryForUpdate() default 86400;

    TimeUnit expiryForUpdateTimeUnit() default TimeUnit.SECONDS;

    // redis

    boolean redisCacheEnabled() default true;

    String[] keyTypeCanonicalName() default {};

    String valueTypeCanonicalName() default "";

    String valueCompressCodec() default "";

    Class<? extends Function<Method, Codec>> keyCodecFactory() default DefaultKeyCodecFactory.class;

    Class<? extends Function<Method, Codec>> valueCodecFactory() default DefaultValueCodecFactory.class;

    boolean keyNotificationEnabled() default true;

    // in-process

    boolean inProcessCacheEnabled() default true;

    int inProcessCacheMaxEntry() default 1024;

    long inProcessCacheExpiryForUpdate() default 86400;

    TimeUnit inProcessCacheExpiryForUpdateTimeUnit() default TimeUnit.SECONDS;

    // custom

    String[] customConfiguration() default {};
}
