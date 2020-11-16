package cc.whohow.redis.jcache;

import javax.cache.management.CacheStatisticsMXBean;

public class RedisTierCacheStatistics implements CacheStatisticsMXBean {
    private final CacheStatisticsMXBean redisCacheStatistics;
    private final CacheStatisticsMXBean inProcessCacheStatistics;

    public RedisTierCacheStatistics(CacheStatisticsMXBean redisCacheStatistics, CacheStatisticsMXBean inProcessCacheStatistics) {
        this.redisCacheStatistics = redisCacheStatistics;
        this.inProcessCacheStatistics = inProcessCacheStatistics;
    }

    @Override
    public void clear() {
        inProcessCacheStatistics.clear();
        redisCacheStatistics.clear();
    }

    @Override
    public long getCacheHits() {
        // 先内存缓存，内存缓存未命中，再Redis缓存
        return inProcessCacheStatistics.getCacheHits() + redisCacheStatistics.getCacheHits();
    }

    @Override
    public float getCacheHitPercentage() {
        long h = getCacheHits();
        long m = getCacheMisses();
        return divide(h, h + m);
    }

    @Override
    public long getCacheMisses() {
        // Redis缓存未命中，才算未命中
        return redisCacheStatistics.getCacheMisses();
    }

    @Override
    public float getCacheMissPercentage() {
        long h = getCacheHits();
        long m = getCacheMisses();
        return divide(m, h + m);
    }

    @Override
    public long getCacheGets() {
        return getCacheHits() + getCacheMisses();
    }

    @Override
    public long getCachePuts() {
        // Redis缓存更新才算更新
        return redisCacheStatistics.getCachePuts();
    }

    @Override
    public long getCacheRemovals() {
        // Redis缓存删除才算删除
        return redisCacheStatistics.getCacheRemovals();
    }

    @Override
    public long getCacheEvictions() {
        // Redis缓存驱逐才算驱逐
        return redisCacheStatistics.getCacheEvictions();
    }

    @Override
    public float getAverageGetTime() {
        // 加权平均
        long inProcessGets = inProcessCacheStatistics.getCacheGets();
        double inProcessAverageGetTime = inProcessCacheStatistics.getAverageGetTime();
        long redisGets = redisCacheStatistics.getCacheGets();
        double redisAverageGetTime = redisCacheStatistics.getAverageGetTime();
        return divide(inProcessGets * inProcessAverageGetTime + redisGets * redisAverageGetTime, inProcessGets + redisGets);
    }

    @Override
    public float getAveragePutTime() {
        return redisCacheStatistics.getAveragePutTime();
    }

    @Override
    public float getAverageRemoveTime() {
        return redisCacheStatistics.getAverageRemoveTime();
    }

    private float divide(double a, long b) {
        return b == 0 ? 0 : (float) (a / b);
    }

    @Override
    public String toString() {
        return "RedisTierCacheStatistics{" +
                "cacheHits=" + getCacheHits() +
                ", cacheMisses=" + getCacheMisses() +
                ", cacheGets=" + getCacheGets() +
                ", cachePuts=" + getCachePuts() +
                ", cacheRemovals=" + getCacheRemovals() +
                ", cacheEvictions=" + getCacheEvictions() +
                ", cacheHitPercentage=" + getCacheHitPercentage() +
                ", cacheMissPercentage=" + getCacheMissPercentage() +
                ", averageGetTime=" + getAverageGetTime() +
                ", averagePutTime=" + getAveragePutTime() +
                ", averageRemoveTime=" + getAverageRemoveTime() +
                "}";
    }
}
