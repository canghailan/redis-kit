package cc.whohow.redis.jcache;

import javax.cache.annotation.GeneratedCacheKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ImmutableGeneratedCacheKey implements GeneratedCacheKey {
    public static ImmutableGeneratedCacheKey empty() {
        return new ImmutableGeneratedCacheKey();
    }

    public static ImmutableGeneratedCacheKey of(Object key) {
        return new ImmutableGeneratedCacheKey(key);
    }

    public static ImmutableGeneratedCacheKey of(Object... keys) {
        return new ImmutableGeneratedCacheKey(keys);
    }

    private final List<Object> keys;

    private ImmutableGeneratedCacheKey(Object... keys) {
        Objects.requireNonNull(keys);
        if (keys.length == 0) {
            this.keys = Collections.emptyList();
        } else if (keys.length == 1) {
            this.keys = Collections.singletonList(keys[0]);
        } else {
            this.keys = Arrays.asList(keys);
        }
    }

    public Object getKey(int index) {
        return keys.get(index);
    }

    public int size() {
        return keys.size();
    }

    public List<Object> getKeys() {
        return keys;
    }

    @Override
    public int hashCode() {
        return keys.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ImmutableGeneratedCacheKey) {
            ImmutableGeneratedCacheKey that = (ImmutableGeneratedCacheKey) o;
            return that.keys.equals(keys);
        }
        return false;
    }

    @Override
    public String toString() {
        return keys.toString();
    }
}
