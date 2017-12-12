package cc.whohow.redis.jcache.configuration;

import javax.cache.configuration.Configuration;

/**
 * @see javax.cache.configuration.Configuration
 * @see javax.cache.configuration.CompleteConfiguration
 */
public interface RedisCacheConfiguration<K, V> extends Configuration<K, V> {
    @Override
    default boolean isStoreByValue() {
        return true;
    }

    boolean isStatisticsEnabled();

    boolean isManagementEnabled();

    long getExpiryForUpdate();
}
