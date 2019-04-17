package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrefixCodec;

/**
 * Redis缓存key编码器，处理cacheName前缀
 */
public class RedisCacheKeyCodec<K> extends PrefixCodec<K> {
    /**
     * 缓存名
     */
    private final String cacheName;
    /**
     * 分隔符
     */
    private final String separator;

    public RedisCacheKeyCodec(String cacheName, String separator, Codec<K> keyCodec) {
        super(keyCodec, cacheName + separator);
        this.cacheName = cacheName;
        this.separator = separator;
    }

    public String getCacheName() {
        return cacheName;
    }

    public String getSeparator() {
        return separator;
    }
}
