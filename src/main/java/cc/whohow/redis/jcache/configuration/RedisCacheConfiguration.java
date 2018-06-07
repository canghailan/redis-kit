package cc.whohow.redis.jcache.configuration;

import javax.cache.configuration.Configuration;
import java.util.List;
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

    boolean isStatisticsEnabled();

    boolean isManagementEnabled();

    long getExpiryForUpdate();

    TimeUnit getExpiryForUpdateTimeUnit();

    // redisClient

    boolean isRedisCacheEnabled();

    String[] getKeyTypeCanonicalName();

    String getValueTypeCanonicalName();

    // in-process

    boolean isInProcessCacheEnabled();

    int getInProcessCacheMaxEntry();

    long getInProcessCacheExpiryForUpdate();

    TimeUnit getInProcessCacheExpiryForUpdateTimeUnit();

    List<String> getCustomConfiguration();
}
