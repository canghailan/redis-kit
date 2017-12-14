package cc.whohow.redis.jcache.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface AnnotationRedisCacheConfiguration {
    String name();

    Class<?> keyType();

    Class<?> valueType();

    long expiryForUpdate() default -1;

    TimeUnit expiryForUpdateTimeUnit() default TimeUnit.SECONDS;

    boolean statisticsEnabled() default true;

    boolean managementEnabled() default false;

    // redis

    boolean redisCacheEnabled() default true;

    String[] keyTypeCanonicalName();

    String valueTypeCanonicalName();

    String keyCodec() default "";

    String valueCodec();

    boolean keyNotificationEnabled() default true;

    // in-process

    boolean inProcessCacheEnabled() default true;

    int inProcessCacheMaxEntry() default -1;

    long inProcessCacheExpiryForUpdate() default -1;

    TimeUnit inProcessCacheExpiryForUpdateTimeUnit() default TimeUnit.SECONDS;
}
