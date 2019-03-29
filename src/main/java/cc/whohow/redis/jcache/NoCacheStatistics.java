package cc.whohow.redis.jcache;

import javax.cache.management.CacheStatisticsMXBean;

public class NoCacheStatistics implements CacheStatisticsMXBean {
    private static final NoCacheStatistics INSTANCE = new NoCacheStatistics();

    public static NoCacheStatistics getInstance() {
        return INSTANCE;
    }

    @Override
    public void clear() {
    }

    @Override
    public long getCacheHits() {
        return 0;
    }

    @Override
    public float getCacheHitPercentage() {
        return 0;
    }

    @Override
    public long getCacheMisses() {
        return 0;
    }

    @Override
    public float getCacheMissPercentage() {
        return 0;
    }

    @Override
    public long getCacheGets() {
        return 0;
    }

    @Override
    public long getCachePuts() {
        return 0;
    }

    @Override
    public long getCacheRemovals() {
        return 0;
    }

    @Override
    public long getCacheEvictions() {
        return 0;
    }

    @Override
    public float getAverageGetTime() {
        return 0;
    }

    @Override
    public float getAveragePutTime() {
        return 0;
    }

    @Override
    public float getAverageRemoveTime() {
        return 0;
    }
}
