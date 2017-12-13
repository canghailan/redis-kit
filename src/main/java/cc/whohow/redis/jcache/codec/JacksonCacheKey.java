package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.jcache.JCacheKey;

import java.lang.reflect.Type;

public class JacksonCacheKey extends JCacheKey {
    protected Type[] cacheKeyTypes;

    public JacksonCacheKey() {
        super(cacheKeys);
    }
}
