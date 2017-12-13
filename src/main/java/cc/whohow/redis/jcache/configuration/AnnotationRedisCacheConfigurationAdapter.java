package cc.whohow.redis.jcache.configuration;

import cc.whohow.redis.jcache.codec.JCacheKeyJacksonCodec;
import cc.whohow.redis.jcache.codec.JacksonCodec;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.redisson.client.codec.*;
import org.redisson.codec.JsonJacksonCodec;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class AnnotationRedisCacheConfigurationAdapter<K, V> implements RedisCacheConfiguration<K, V> {
    protected static final TypeFactory TYPE_FACTORY = TypeFactory.defaultInstance();
    protected final AnnotationRedisCacheConfiguration cacheConfiguration;

    public AnnotationRedisCacheConfigurationAdapter(AnnotationRedisCacheConfiguration cacheConfiguration) {
        this.cacheConfiguration = cacheConfiguration;
    }

    @Override
    public String getName() {
        return cacheConfiguration.name();
    }

    @Override
    public long getExpiryForUpdate() {
        return cacheConfiguration.expiryForUpdate();
    }

    @Override
    public TimeUnit getExpiryForUpdateTimeUnit() {
        return cacheConfiguration.expiryForUpdateTimeUnit();
    }

    @Override
    public boolean isStatisticsEnabled() {
        return cacheConfiguration.statisticsEnabled();
    }

    @Override
    public boolean isManagementEnabled() {
        return cacheConfiguration.managementEnabled();
    }

    @Override
    public boolean isRedisCacheEnabled() {
        return cacheConfiguration.redisCacheEnabled();
    }

    @Override
    public String getRedisKey() {
        return cacheConfiguration.redisKey();
    }

    @Override
    public Codec getKeyCodec() {
        if (cacheConfiguration.keyCodec().isEmpty()) {
            return getDefaultKeyCodec();
        }
        return null;
    }

    @Override
    public Codec getValueCodec() {
        return null;
    }

    @Override
    public String[] getKeyTypeCanonicalName() {
        return cacheConfiguration.keyTypeCanonicalName();
    }

    @Override
    public String getValueTypeCanonicalName() {
        return cacheConfiguration.valueTypeCanonicalName();
    }

    public Codec getDefaultKeyCodec() {
        String[] keyTypeCanonicalName = getKeyTypeCanonicalName();
        if (keyTypeCanonicalName == null || keyTypeCanonicalName.length == 0) {
            throw new IllegalStateException();
        }
        if (keyTypeCanonicalName.length == 1) {
            Type keyType = TYPE_FACTORY.constructFromCanonical(keyTypeCanonicalName[0]);
            if (keyType == String.class) {
                return StringCodec.INSTANCE;
            }
            if (keyType == Integer.class || keyType == int.class) {
                return IntegerCodec.INSTANCE;
            }
            if (keyType == Long.class || keyType == long.class) {
                return LongCodec.INSTANCE;
            }
            if (keyType == byte[].class) {
                return ByteArrayCodec.INSTANCE;
            }
            return new JacksonCodec(keyType);
        }
        Type[] keyType = Arrays.stream(getKeyTypeCanonicalName())
                .map(TYPE_FACTORY::constructFromCanonical)
                .toArray(Type[]::new);
        return new JCacheKeyJacksonCodec(keyType);
    }

    public Codec getDefaultValueCodec() {
        return JsonJacksonCodec.INSTANCE;
    }

    @Override
    public boolean isPublishCacheEntryEventEnabled() {
        return cacheConfiguration.publishCacheEntryEventEnabled();
    }

    @Override
    public boolean isInProcessCacheEnabled() {
        return cacheConfiguration.inProcessCacheEnabled();
    }

    @Override
    public int getInProcessCacheMaxEntry() {
        return cacheConfiguration.inProcessCacheMaxEntry();
    }

    @Override
    public long getInProcessCacheExpiryForUpdate() {
        return cacheConfiguration.inProcessCacheExpiryForUpdate();
    }

    @Override
    public TimeUnit getInProcessCacheExpiryForUpdateTimeUnit() {
        return cacheConfiguration.inProcessCacheExpiryForUpdateTimeUnit();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<K> getKeyType() {
        return (Class<K>) cacheConfiguration.keyType();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<V> getValueType() {
        return (Class<V>) cacheConfiguration.valueType();
    }
}
