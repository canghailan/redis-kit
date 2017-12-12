package cc.whohow.redis.jcache;

import javax.cache.annotation.CacheKey;
import java.util.Arrays;

public class JCacheKey {
    protected CacheKey[] cacheKeys;
    protected Object[] cacheKeyObjects;

    @Override
    public int hashCode() {
        return Arrays.hashCode(cacheKeyObjects);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JCacheKey) {
            JCacheKey that = (JCacheKey) o;
            return Arrays.equals(this.cacheKeyObjects, that.cacheKeyObjects);
        }
        return false;
    }

    @Override
    public String toString() {
        return Arrays.toString(cacheKeyObjects);
    }
}
