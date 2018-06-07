package cc.whohow.redis.jcache;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCachingProvider implements CachingProvider {
    private static final RedisCachingProvider INSTANCE = new RedisCachingProvider();
    private final Map<String, RedisCacheManager> cacheManagers = new ConcurrentHashMap<>();
    private volatile RedisCacheManager defaultCacheManager;

    private RedisCachingProvider() {
    }

    public static RedisCachingProvider getInstance() {
        return INSTANCE;
    }

    @Override
    public RedisCacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        return cacheManagers.get(getCacheManagerIdentifier(uri));
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        return URI.create("redisClient://");
    }

    @Override
    public Properties getDefaultProperties() {
        return new Properties();
    }

    @Override
    public RedisCacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return getCacheManager(uri, classLoader, null);
    }

    @Override
    public RedisCacheManager getCacheManager() {
        return defaultCacheManager;
    }

    public void addCacheManager(RedisCacheManager cacheManager) {
        String id = getCacheManagerIdentifier(cacheManager.getURI());
        if (cacheManagers.putIfAbsent(id, cacheManager) != null) {
            throw new IllegalStateException();
        }
        if (defaultCacheManager == null) {
            defaultCacheManager = cacheManager;
        }
    }

    public void setDefaultCacheManager(RedisCacheManager cacheManager) {
        String id = getCacheManagerIdentifier(cacheManager.getURI());
        cacheManagers.putIfAbsent(id, cacheManager);
        defaultCacheManager = cacheManager;
    }

    @Override
    public void close() {
        cacheManagers.values().forEach(this::closeQuietly);
    }

    @Override
    public void close(ClassLoader classLoader) {
        close();
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        closeQuietly(getCacheManager(uri, classLoader));
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }

    protected String getCacheManagerIdentifier(URI uri) {
        return uri.getScheme() + "://" + uri.getHost() + ":" + Objects.toString(uri.getPort(), "6379") + uri.getPath();
    }

    protected void closeQuietly(CacheManager cacheManager) {
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Throwable ignore) {
            }
        }
    }

    @Override
    public String toString() {
        return "RedisCachingProvider{" +
                "cacheManagers=" + cacheManagers +
                ", defaultCacheManager=" + defaultCacheManager +
                '}';
    }
}
