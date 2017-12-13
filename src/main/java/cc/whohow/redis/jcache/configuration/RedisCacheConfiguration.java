package cc.whohow.redis.jcache.configuration;

import org.redisson.client.codec.Codec;

import javax.cache.configuration.Configuration;
import java.util.concurrent.TimeUnit;

/**
 * @see javax.cache.configuration.Configuration
 * @see javax.cache.configuration.CompleteConfiguration
 */
public interface RedisCacheConfiguration<K, V> extends Configuration<K, V> {
    @Override
    default boolean isStoreByValue() {
        return true;
    }

    String getName();

    long getExpiryForUpdate();

    TimeUnit getExpiryForUpdateTimeUnit();

    boolean isStatisticsEnabled();

    boolean isManagementEnabled();

    // redis

    boolean isRedisCacheEnabled();

    String getRedisKey();

    Codec getKeyCodec();

    Codec getValueCodec();

    String[] getKeyTypeCanonicalName();

    String getValueTypeCanonicalName();

    boolean isPublishCacheEntryEventEnabled();

    // in-process

    boolean isInProcessCacheEnabled();

    int getInProcessCacheMaxEntry();

    long getInProcessCacheExpiryForUpdate();

    TimeUnit getInProcessCacheExpiryForUpdateTimeUnit();
}
