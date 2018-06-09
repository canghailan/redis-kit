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

    String[] getKeyTypeCanonicalName();

    String getValueTypeCanonicalName();

    String getKeyCodec();

    String getValueCodec();

    boolean isStatisticsEnabled();

    boolean isManagementEnabled();

    long getExpiryForUpdate();

    TimeUnit getExpiryForUpdateTimeUnit();

    // redis

    boolean isRedisCacheEnabled();

    // in-process

    boolean isInProcessCacheEnabled();

    int getInProcessCacheMaxEntry();

    long getInProcessCacheExpiryForUpdate();

    TimeUnit getInProcessCacheExpiryForUpdateTimeUnit();

    // extra

    List<String> getExtraConfigurations();
}
