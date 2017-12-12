package cc.whohow.redis.jcache.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface AnnotationRedisCacheConfiguration {
    boolean redisCacheEnabled() default true;

    String name();

    Class<?> keyType();

    Class<?> valueType();

    String[] keyTypeCanonicalName();

    String valueTypeCanonicalName();

    String keyCodec();

    String valueCodec();

    long expiryForUpdate() default -1;

    TimeUnit expiryForUpdateTimeUnit() default TimeUnit.SECONDS;

    boolean inProcessCacheEnabled() default true;

    int inProcessCacheMaxEntry() default -1;

    long inProcessCacheExpiryForUpdate() default -1;

    TimeUnit inProcessCacheExpiryForUpdateTimeUnit() default TimeUnit.SECONDS;

    boolean publishCacheEntryEventEnabled() default true;

    boolean statisticsEnabled() default true;

    boolean managementEnabled() default false;
}
