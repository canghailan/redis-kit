package cc.whohow.redis.jcache.annotation;


import cc.whohow.redis.jcache.codec.DefaultRedisCacheCodecFactory;
import cc.whohow.redis.jcache.codec.RedisCacheCodecFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Redis缓存配置
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface RedisCacheable {
    boolean statisticsEnabled() default true;

    boolean managementEnabled() default false;

    long expiryForUpdate() default 7 * 24 * 60 * 60;

    TimeUnit expiryForUpdateTimeUnit() default TimeUnit.SECONDS;

    String[] keyTypeCanonicalName() default {};

    String valueTypeCanonicalName() default "";

    Class<? extends RedisCacheCodecFactory> redisCacheCodecFactory() default DefaultRedisCacheCodecFactory.class;

    boolean redisCacheEnabled() default true;

    boolean inProcessCacheEnabled() default true;

    int inProcessCacheMaxEntry() default 1024;

    long inProcessCacheExpiryForUpdate() default 5 * 60;

    TimeUnit inProcessCacheExpiryForUpdateTimeUnit() default TimeUnit.SECONDS;

    String[] custom() default {};
}
