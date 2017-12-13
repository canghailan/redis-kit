package cc.whohow.redis.jcache;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;

public class RedisCachingProvider implements CachingProvider {
    private RedisCacheManager cacheManager;

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        return cacheManager;
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        return URI.create("redis://");
    }

    @Override
    public Properties getDefaultProperties() {
        return new Properties();
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return cacheManager;
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    public void setCacheManager(RedisCacheManager cacheManager) {
        if (this.cacheManager != null) {
            throw new IllegalStateException();
        }
        this.cacheManager = cacheManager;
    }

    @Override
    public void close() {
        cacheManager.close();
    }

    @Override
    public void close(ClassLoader classLoader) {
        cacheManager.close();
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        cacheManager.close();
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }
}
