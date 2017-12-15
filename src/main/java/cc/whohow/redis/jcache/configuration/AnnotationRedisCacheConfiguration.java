package cc.whohow.redis.jcache.configuration;

import cc.whohow.redis.jcache.annotation.RedisCacheDefaults;
import cc.whohow.redis.jcache.util.CacheMethods;
import org.redisson.client.codec.Codec;

import javax.cache.annotation.CacheResult;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.TimeUnit;

public class AnnotationRedisCacheConfiguration<K, V> implements RedisCacheConfiguration<K, V> {
    private Method method;
    private CacheResult cacheResult;
    private RedisCacheDefaults redisCacheDefaults;
    private String[] keyTypeCanonicalName;
    private String valueTypeCanonicalName;
    private Class<?> keyType;
    private Class<?> valueType;
    private Codec keyCodec;
    private Codec valueCodec;

    public AnnotationRedisCacheConfiguration(Method method, RedisCacheDefaults redisCacheDefaults) {
        this.method = method;
        this.cacheResult = method.getAnnotation(CacheResult.class);
        this.redisCacheDefaults = redisCacheDefaults;
        this.keyTypeCanonicalName = redisCacheDefaults.keyTypeCanonicalName();
        if (this.keyTypeCanonicalName.length == 0) {
            this.keyTypeCanonicalName = CacheMethods.getKeyTypeCanonicalName(method);
        }
        this.valueTypeCanonicalName = redisCacheDefaults.valueTypeCanonicalName();
        if (this.valueTypeCanonicalName.isEmpty()) {
            this.valueTypeCanonicalName = CacheMethods.getValueTypeCanonicalName(method);
        }
        this.keyType = redisCacheDefaults.keyType();
        this.valueType = redisCacheDefaults.valueType();
    }

    @Override
    public String getName() {
        return cacheResult.cacheName();
    }

    @Override
    public boolean isStatisticsEnabled() {
        return redisCacheDefaults.statisticsEnabled();
    }

    @Override
    public boolean isManagementEnabled() {
        return redisCacheDefaults.managementEnabled();
    }

    @Override
    public long getExpiryForUpdate() {
        return redisCacheDefaults.expiryForUpdate();
    }

    @Override
    public TimeUnit getExpiryForUpdateTimeUnit() {
        return redisCacheDefaults.expiryForUpdateTimeUnit();
    }

    @Override
    public boolean isRedisCacheEnabled() {
        return redisCacheDefaults.redisCacheEnabled();
    }

    @Override
    public boolean isKeyNotificationEnabled() {
        return redisCacheDefaults.keyNotificationEnabled();
    }

    @Override
    public Codec getKeyCodec() {
        if (keyCodec == null) {
            try {
                keyCodec = redisCacheDefaults.keyCodecFactory().getDeclaredConstructor().newInstance().apply(method);
            } catch (Exception e) {
                throw new UndeclaredThrowableException(e);
            }
        }
        return keyCodec;
    }

    @Override
    public Codec getValueCodec() {
        if (valueCodec == null) {
            try {
                valueCodec = redisCacheDefaults.valueCodecFactory().getDeclaredConstructor().newInstance().apply(method);
            } catch (Exception e) {
                throw new UndeclaredThrowableException(e);
            }
        }
        return valueCodec;
    }

    @Override
    public String[] getKeyTypeCanonicalName() {
        return keyTypeCanonicalName;
    }

    @Override
    public String getValueTypeCanonicalName() {
        return valueTypeCanonicalName;
    }

    @Override
    public boolean isInProcessCacheEnabled() {
        return redisCacheDefaults.inProcessCacheEnabled();
    }

    @Override
    public int getInProcessCacheMaxEntry() {
        return redisCacheDefaults.inProcessCacheMaxEntry();
    }

    @Override
    public long getInProcessCacheExpiryForUpdate() {
        return redisCacheDefaults.inProcessCacheExpiryForUpdate();
    }

    @Override
    public TimeUnit getInProcessCacheExpiryForUpdateTimeUnit() {
        return redisCacheDefaults.inProcessCacheExpiryForUpdateTimeUnit();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<K> getKeyType() {
        return (Class<K>) keyType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<V> getValueType() {
        return (Class<V>) valueType;
    }
}
