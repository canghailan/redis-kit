package cc.whohow.redis;

import cc.whohow.redis.jcache.Cache;
import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;
import cc.whohow.redis.jcache.RedisCacheManager;
import cc.whohow.redis.jcache.configuration.MutableRedisCacheConfiguration;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.util.RedisKeyspaceNotification;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.cache.annotation.GeneratedCacheKey;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class TestCache {
    private static final RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;
    private static Redis redis;
    private static RedisTracking redisTracking;
    private static RedisCacheManager cacheManager;
    private static Cache<GeneratedCacheKey, Data> cache;

    @BeforeClass
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
            redis = new SingleRedis(redisClient, redisURI);

            redisTracking = new RedisKeyspaceNotification(redisClient, redisURI);

            MutableRedisCacheConfiguration configuration = new MutableRedisCacheConfiguration<>();
            configuration.setName("c.w.Test");
            configuration.setKeyTypeCanonicalName(new String[]{String.class.getCanonicalName()});
            configuration.setValueTypeCanonicalName(Data.class.getCanonicalName());

            Map<String, RedisCacheConfiguration> cacheConfigurationMap = new HashMap<>();
            cacheConfigurationMap.put(configuration.getName(), configuration);

            cacheManager = new RedisCacheManager(redis, redisTracking, cacheConfigurationMap::get);

            cache = cacheManager.createCache(configuration.getName(), configuration);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cacheManager.close();
        redisTracking.close();
        redis.close();
        redisClient.shutdown();
    }

    protected Data random() {
        Data data = new Data();
        data.a = Character.valueOf((char) ThreadLocalRandom.current().nextInt('a', 'z')).toString();
        data.b = ThreadLocalRandom.current().nextInt(0, 100);
        data.c = ThreadLocalRandom.current().nextLong(0, 1000);
        data.d = new Date();
        return data;
    }

    @Test
    public void testGet() throws Exception {
        GeneratedCacheKey a = ImmutableGeneratedCacheKey.of("a");

        Data randomA = random();

        cache.put(a, randomA);
        Data dataA = cache.get(a);

        System.out.println(dataA);

        Assert.assertEquals(randomA, dataA);
    }

    @Test
    public void testRemove() {
        GeneratedCacheKey a = ImmutableGeneratedCacheKey.of("a");

        cache.put(a, random());
        cache.remove(a);

        Assert.assertNull(cache.get(a));
    }
}
