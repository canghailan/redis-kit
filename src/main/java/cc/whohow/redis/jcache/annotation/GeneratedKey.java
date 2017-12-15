package cc.whohow.redis.jcache.annotation;

import javax.cache.annotation.GeneratedCacheKey;
import java.util.Arrays;

public class GeneratedKey implements GeneratedCacheKey {
    protected final Object[] keys;

    protected GeneratedKey(Object... keys) {
        this.keys = keys;
    }

    public static GeneratedKey of(Object... keys) {
        if (keys.length == 1) {
            return new GeneratedSimpleKey(keys);
        } else {
            return new GeneratedKey(keys);
        }
    }

    public Object[] getKeys() {
        return keys;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(keys);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof GeneratedKey) {
            GeneratedKey that = (GeneratedKey) o;
            return Arrays.equals(this.keys, that.keys);
        }
        return false;
    }

    @Override
    public String toString() {
        return Arrays.toString(keys);
    }
}
