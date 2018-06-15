package cc.whohow.redis.jcache.configuration;

import cc.whohow.redis.jcache.annotation.RedisCacheable;
import cc.whohow.redis.jcache.codec.RedisCacheCodecFactory;
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

/**
 * 基于RedisCacheable注解的缓存配置
 */
public class AnnotationRedisCacheConfiguration implements RedisCacheConfiguration {
    private final CacheMethodDetails<? extends Annotation> cacheMethodDetails;
    private final Method method;
    private final RedisCacheable redisCacheable;

    public AnnotationRedisCacheConfiguration(CacheMethodDetails<? extends Annotation> cacheMethodDetails) {
        this.cacheMethodDetails = cacheMethodDetails;
        this.method = cacheMethodDetails.getMethod();
        this.redisCacheable = method.getAnnotation(RedisCacheable.class);
    }

    @Override
    public String getName() {
        return cacheMethodDetails.getCacheName();
    }

    public String[] getKeyTypeCanonicalName() {
        if (redisCacheable.keyTypeCanonicalName().length == 0) {
            return Arrays.stream(getGenericKeyType())
                    .map(TypeFactory.defaultInstance()::constructType)
                    .map(JavaType::toCanonical)
                    .toArray(String[]::new);
        }
        return redisCacheable.keyTypeCanonicalName();
    }

    /**
     * 缓存Key类型
     */
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

    public String getValueTypeCanonicalName() {
        if (redisCacheable.valueTypeCanonicalName().isEmpty()) {
            return TypeFactory.defaultInstance().constructType(getGenericValueType()).toCanonical();
        }
        return redisCacheable.valueTypeCanonicalName();
    }

    /**
     * 缓存值类型
     */
    public Type getGenericValueType() {
        return method.getGenericReturnType();
    }

    public Class<? extends RedisCacheCodecFactory> getRedisCacheCodecFactory() {
        return redisCacheable.redisCacheCodecFactory();
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
    public List<String> getExConfigurations() {
        return Arrays.asList(redisCacheable.ex());
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
