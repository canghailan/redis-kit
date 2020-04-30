package cc.whohow.redis.jcache.configuration;

import cc.whohow.redis.jcache.codec.RedisCacheCodecFactory;

import javax.cache.configuration.Configuration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Redis缓存配置项
 *
 * @see javax.cache.configuration.Configuration
 * @see javax.cache.configuration.CompleteConfiguration
 */
public interface RedisCacheConfiguration<K, V> extends Configuration<K, V> {
    @Override
    default boolean isStoreByValue() {
        return true;
    }

    default String getRedisKeySeparator() {
        return getKeyTypeCanonicalName().length == 0 ? "" : ":";
    }

    default String getRedisKeyPrefix() {
        if (getKeyTypeCanonicalName().length == 0) {
            return getName();
        } else {
            return getName() + getRedisKeySeparator();
        }
    }

    default String getRedisKeyPattern() {
        if (getKeyTypeCanonicalName().length == 0) {
            return getName();
        } else {
            return getName() + getRedisKeySeparator() + "*";
        }
    }

    /**
     * 缓存名
     */
    String getName();

    /**
     * 缓存键类型名
     */
    String[] getKeyTypeCanonicalName();

    /**
     * 缓存值类型名
     */
    String getValueTypeCanonicalName();

    /**
     * 缓存键编码器
     */
    Class<? extends RedisCacheCodecFactory> getRedisCacheCodecFactory();

    /**
     * 是否启用缓存统计
     */
    boolean isStatisticsEnabled();

    /**
     * 是否启用缓存MXBean
     */
    boolean isManagementEnabled();

    /**
     * 过期时间
     */
    long getExpiryForUpdate();

    /**
     * 过期时间单位
     */
    TimeUnit getExpiryForUpdateTimeUnit();

    /**
     * 是否启用Redis缓存
     */
    boolean isRedisCacheEnabled();

    /**
     * 是否启用内存缓存
     */
    boolean isInProcessCacheEnabled();

    /**
     * 内存缓存最大数量
     */
    int getInProcessCacheMaxEntry();

    /**
     * 内存缓存过期时间
     */
    long getInProcessCacheExpiryForUpdate();

    /**
     * 内存缓存过期时间单位
     */
    TimeUnit getInProcessCacheExpiryForUpdateTimeUnit();

    /**
     * 自定义参数
     */
    List<String> getCustom();
}
