package cc.whohow.redis.jcache.annotation;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface RedisCacheResult {
    boolean statisticsEnabled() default true;

    boolean managementEnabled() default false;

    long expiryForUpdate() default 7 * 24 * 60 * 60;

    TimeUnit expiryForUpdateTimeUnit() default TimeUnit.SECONDS;

    String[] keyTypeCanonicalName() default {};

    String valueTypeCanonicalName() default "";

    String keyCodec() default "";

    String valueCodec() default "";

    boolean redisCacheEnabled() default true;

    boolean inProcessCacheEnabled() default true;

    int inProcessCacheMaxEntry() default 1024;

    long inProcessCacheExpiryForUpdate() default 24 * 60 * 60;

    TimeUnit inProcessCacheExpiryForUpdateTimeUnit() default TimeUnit.SECONDS;

    String[] extra() default {};
}