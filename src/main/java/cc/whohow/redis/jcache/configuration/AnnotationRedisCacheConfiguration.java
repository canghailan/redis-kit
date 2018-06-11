package cc.whohow.redis.jcache.configuration;

import cc.whohow.redis.jcache.annotation.RedisCacheable;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheMethodDetails;
import javax.cache.annotation.GeneratedCacheKey;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AnnotationRedisCacheConfiguration implements RedisCacheConfiguration {
    private final CacheMethodDetails<? extends Annotation> cacheMethodDetails;
    private final Method method;
    private final RedisCacheable redisCacheable;

    public AnnotationRedisCacheConfiguration(CacheMethodDetails<? extends Annotation> cacheMethodDetails) {
        this.cacheMethodDetails = cacheMethodDetails;
        this.method = cacheMethodDetails.getMethod();
        this.redisCacheable = method.getAnnotation(RedisCacheable.class);
    }

    public Type[] getGenericKeyType() {
        List<Type> result = new ArrayList<>(method.getParameterCount());
        Type[] parameterTypes = method.getGenericParameterTypes();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int i = 0; i < parameterTypes.length; i++) {
            Annotation[] annotations = parameterAnnotations[i];
            for (Annotation annotation : annotations) {
                if (annotation.annotationType() == CacheKey.class) {
                    result.add(parameterTypes[i]);
                    break;
                }
            }
        }
        if (result.isEmpty()) {
            return parameterTypes;
        } else {
            return result.toArray(new Type[0]);
        }
    }

    @Override
    public String getName() {
        return cacheMethodDetails.getCacheName();
    }

    public String[] getKeyTypeCanonicalName() {
        return Arrays.stream(getGenericKeyType())
                .map(TypeFactory.defaultInstance()::constructType)
                .map(JavaType::toCanonical)
                .toArray(String[]::new);
    }

    public Type getGenericValueType() {
        return method.getGenericReturnType();
    }

    public String getValueTypeCanonicalName() {
        return TypeFactory.defaultInstance().constructType(getGenericValueType()).toCanonical();
    }

    @Override
    public String getKeyCodec() {
        return redisCacheable.keyCodec();
    }

    @Override
    public String getValueCodec() {
        return redisCacheable.valueCodec();
    }

    @Override
    public boolean isStatisticsEnabled() {
        return redisCacheable.statisticsEnabled();
    }

    @Override
    public boolean isManagementEnabled() {
        return redisCacheable.managementEnabled();
    }

    @Override
    public long getExpiryForUpdate() {
        return redisCacheable.expiryForUpdate();
    }

    @Override
    public TimeUnit getExpiryForUpdateTimeUnit() {
        return redisCacheable.expiryForUpdateTimeUnit();
    }

    @Override
    public boolean isRedisCacheEnabled() {
        return redisCacheable.redisCacheEnabled();
    }

    @Override
    public boolean isInProcessCacheEnabled() {
        return redisCacheable.inProcessCacheEnabled();
    }

    @Override
    public int getInProcessCacheMaxEntry() {
        return redisCacheable.inProcessCacheMaxEntry();
    }

    @Override
    public long getInProcessCacheExpiryForUpdate() {
        return redisCacheable.inProcessCacheExpiryForUpdate();
    }

    @Override
    public TimeUnit getInProcessCacheExpiryForUpdateTimeUnit() {
        return redisCacheable.inProcessCacheExpiryForUpdateTimeUnit();
    }

    @Override
    public List<String> getExtraConfigurations() {
        return Arrays.asList(redisCacheable.extra());
    }

    @Override
    public Class<?> getKeyType() {
        return GeneratedCacheKey.class;
    }

    @Override
    public Class<?> getValueType() {
        return method.getReturnType();
    }
}
