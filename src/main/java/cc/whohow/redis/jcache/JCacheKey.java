package cc.whohow.redis.jcache;

import java.util.Arrays;

public class JCacheKey {
    protected final Object[] cacheKeys;

    public JCacheKey(Object[] cacheKeys) {
        this.cacheKeys = cacheKeys;
    }

    public Object[] getCacheKeys() {
        return cacheKeys;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(cacheKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JCacheKey) {
            JCacheKey that = (JCacheKey) o;
            return Arrays.equals(this.cacheKeys, that.cacheKeys);
        }
        return false;
    }

    @Override
    public String toString() {
        return Arrays.toString(cacheKeys);
    }
}
