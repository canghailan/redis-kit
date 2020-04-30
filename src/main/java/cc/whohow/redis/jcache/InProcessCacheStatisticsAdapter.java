package cc.whohow.redis.jcache;

import com.github.benmanes.caffeine.cache.Cache;

import javax.cache.management.CacheStatisticsMXBean;

public class InProcessCacheStatisticsAdapter implements CacheStatisticsMXBean {
    protected Cache<?, ?> cache;

    public InProcessCacheStatisticsAdapter(Cache<?, ?> cache) {
        this.cache = cache;
    }

    @Override
    public void clear() {
    }

    @Override
    public long getCacheHits() {
        return cache.stats().hitCount();
    }

    @Override
    public float getCacheHitPercentage() {
        return (float) cache.stats().hitRate();
    }

    @Override
    public long getCacheMisses() {
        return cache.stats().missCount();
    }

    @Override
    public float getCacheMissPercentage() {
        return (float) cache.stats().missRate();
    }

    @Override
    public long getCacheGets() {
        return cache.stats().requestCount();
    }

    @Override
    public long getCachePuts() {
        return cache.stats().requestCount();
    }

    @Override
    public long getCacheRemovals() {
        return cache.stats().loadCount();
    }

    @Override
    public long getCacheEvictions() {
        return cache.stats().evictionCount();
    }

    @Override
    public float getAverageGetTime() {
        return 0;
    }

    @Override
    public float getAveragePutTime() {
        return (float) cache.stats().averageLoadPenalty();
    }

    @Override
    public float getAverageRemoveTime() {
        return 0;
    }

    @Override
    public String toString() {
        return cache.stats().toString();
    }
}
