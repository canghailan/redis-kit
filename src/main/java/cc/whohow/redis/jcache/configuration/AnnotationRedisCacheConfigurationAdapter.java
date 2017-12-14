package cc.whohow.redis.jcache.configuration;

import cc.whohow.redis.jcache.codec.JCacheKeyJacksonCodec;
import cc.whohow.redis.jcache.codec.ObjectArrayJacksonCodec;
import cc.whohow.redis.jcache.codec.ObjectJacksonCodec;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.redisson.client.codec.*;

import java.util.concurrent.TimeUnit;

public class AnnotationRedisCacheConfigurationAdapter<K, V> implements RedisCacheConfiguration<K, V> {
    protected final AnnotationRedisCacheConfiguration cacheConfiguration;

    public AnnotationRedisCacheConfigurationAdapter(AnnotationRedisCacheConfiguration cacheConfiguration) {
        this.cacheConfiguration = cacheConfiguration;
    }

    @Override
    public String getName() {
        return cacheConfiguration.name();
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
    public long getExpiryForUpdate() {
        return cacheConfiguration.expiryForUpdate();
    }

    @Override
    public TimeUnit getExpiryForUpdateTimeUnit() {
        return cacheConfiguration.expiryForUpdateTimeUnit();
    }

    @Override
    public boolean isRedisCacheEnabled() {
        return cacheConfiguration.redisCacheEnabled();
    }

    @Override
    public boolean isKeyNotificationEnabled() {
        return cacheConfiguration.keyNotificationEnabled();
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
        } else if (keyTypeCanonicalName.length == 1) {
            return getDefaultCodec(keyTypeCanonicalName[0]);
        } else {
            return new JCacheKeyJacksonCodec(new ObjectArrayJacksonCodec(keyTypeCanonicalName));
        }
    }

    public Codec getDefaultValueCodec() {
        return getDefaultCodec(getValueTypeCanonicalName());
    }

    private Codec getDefaultCodec(String typeCanonicalName) {
        if (typeCanonicalName == null || typeCanonicalName.isEmpty()) {
            throw new IllegalArgumentException();
        }
        switch (typeCanonicalName) {
            case "java.lang.String":
                return StringCodec.INSTANCE;
            case "int":
                return IntegerCodec.INSTANCE;
            case "java.lang.Integer":
                return IntegerCodec.INSTANCE;
            case "long":
                return LongCodec.INSTANCE;
            case "java.lang.Long":
                return LongCodec.INSTANCE;
            case "byte[]":
                return ByteArrayCodec.INSTANCE;
            default:
                return new ObjectJacksonCodec(typeCanonicalName);
        }
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
