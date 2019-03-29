package cc.whohow.redis.jcache;

import javax.cache.management.CacheStatisticsMXBean;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

public class CacheStats implements CacheStatisticsMXBean {
    protected final LongAdder cacheHits = new LongAdder();
    protected final LongAdder cacheMisses = new LongAdder();
    protected final LongAdder cachePuts = new LongAdder();
    protected final LongAdder cacheRemovals = new LongAdder();
    protected final LongAdder cacheEvictions = new LongAdder();
    protected final DoubleAdder cacheGetTime = new DoubleAdder();
    protected final DoubleAdder cachePutTime = new DoubleAdder();
    protected final DoubleAdder cacheRemoveTime = new DoubleAdder();

    public void cacheGet(int hits, int misses, long time) {
        cacheHits.add(hits);
        cacheMisses.add(misses);
        cacheGetTime.add(time);
    }

    public void cacheHit(int hits, long time) {
        cacheHits.add(hits);
        cacheGetTime.add(time);
    }

    public void cacheMiss(int misses, long time) {
        cacheMisses.add(misses);
        cacheGetTime.add(time);
    }

    public void cachePut(int puts, long time) {
        cachePuts.add(puts);
        cachePutTime.add(time);
    }

    public void cacheRemove(int removals, long time) {
        cacheRemovals.add(removals);
        cacheRemoveTime.add(time);
    }

    public void cacheEvict(int evictions) {
        cacheEvictions.add(evictions);
    }

    @Override
    public void clear() {
        cacheHits.reset();
        cacheMisses.reset();
        cachePuts.reset();
        cacheRemovals.reset();
        cacheEvictions.reset();
        cacheGetTime.reset();
        cachePutTime.reset();
        cacheRemoveTime.reset();
    }

    @Override
    public long getCacheHits() {
        return cacheHits.longValue();
    }

    @Override
    public float getCacheHitPercentage() {
        long h = cacheHits.longValue();
        long m = cacheMisses.longValue();
        return divide(h, h + m);
    }

    @Override
    public long getCacheMisses() {
        return cacheMisses.longValue();
    }

    @Override
    public float getCacheMissPercentage() {
        long h = cacheHits.longValue();
        long m = cacheMisses.longValue();
        return divide(m, h + m);
    }

    @Override
    public long getCacheGets() {
        return getCacheHits() + getCacheMisses();
    }

    @Override
    public long getCachePuts() {
        return cachePuts.longValue();
    }

    @Override
    public long getCacheRemovals() {
        return cacheRemovals.longValue();
    }

    @Override
    public long getCacheEvictions() {
        return cacheEvictions.longValue();
    }

    @Override
    public float getAverageGetTime() {
        return divide(cacheGetTime.floatValue(), getCacheGets());
    }

    @Override
    public float getAveragePutTime() {
        return divide(cachePutTime.floatValue(), getCachePuts());
    }

    @Override
    public float getAverageRemoveTime() {
        return divide(cacheRemoveTime.floatValue(), getCacheRemovals());
    }

    private float divide(float a, long b) {
        return b == 0 ? 0 : a / b;
    }

    @Override
    public String toString() {
        return "CacheStats{" +
                "cacheHits=" + cacheHits +
                ", cacheMisses=" + cacheMisses +
                ", cachePuts=" + cachePuts +
                ", cacheRemovals=" + cacheRemovals +
                ", cacheEvictions=" + cacheEvictions +
                ", cacheGetTime=" + cacheGetTime +
                ", cachePutTime=" + cachePutTime +
                ", cacheRemoveTime=" + cacheRemoveTime +
                ", cacheHitPercentage=" + getCacheHitPercentage() +
                ", cacheMissPercentage=" + getCacheMissPercentage() +
                ", cacheGets=" + getCacheGets() +
                ", averageGetTime=" + getAverageGetTime() +
                ", averagePutTime=" + getAveragePutTime() +
                ", averageRemoveTime=" + getAverageRemoveTime() +
                '}';
    }
}
