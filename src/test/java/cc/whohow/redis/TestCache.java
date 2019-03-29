package cc.whohow.redis;

import cc.whohow.redis.jcache.Cache;
import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;
import cc.whohow.redis.jcache.RedisCacheManager;
import cc.whohow.redis.jcache.configuration.MutableRedisCacheConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.cache.annotation.GeneratedCacheKey;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

public class TestCache {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;
    private static RedisCacheManager cacheManager;
    private static Cache<GeneratedCacheKey, Data> cache;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
            cacheManager = new RedisCacheManager(redisClient, redisURI);

            MutableRedisCacheConfiguration configuration = new MutableRedisCacheConfiguration<>();
            configuration.setName("c.w.Test");
            configuration.setKeyTypeCanonicalName(new String[]{String.class.getCanonicalName()});
            configuration.setValueTypeCanonicalName(Data.class.getCanonicalName());
            cache = cacheManager.createCache(configuration.getName(), configuration);
        }
    }

    @AfterClass
    public static void tearDown() {
        cacheManager.close();
        redisClient.shutdown();
    }

    @Test
    public void testSet() {
        Data data = new Data();
        data.a = "a";
        data.b = 2;
        data.c = 5;
        data.d = new Date();
        cache.put(ImmutableGeneratedCacheKey.of("a"), data);
    }

    @Test
    public void testGet() throws Exception {
        Object data1 = cache.get(ImmutableGeneratedCacheKey.of("a"));
        System.out.println(data1);
        System.out.println(objectMapper.writeValueAsString(data1));

        Object data2 = cache.get(ImmutableGeneratedCacheKey.of("b"));
        System.out.println(data2);
        System.out.println(objectMapper.writeValueAsString(data2));

        System.out.println(cache.getValue(ImmutableGeneratedCacheKey.of("a")));
        System.out.println(cache.getValue(ImmutableGeneratedCacheKey.of("b")));
    }

    @Test
    public void testRemove() {
        cache.remove(ImmutableGeneratedCacheKey.of("a"));
        cache.remove(ImmutableGeneratedCacheKey.of("b"));
    }

    @Test
    public void testSetupKeyspaceNotification() {
        cacheManager.enableKeyspaceNotification();
    }

    @Test
    public void testPubSub() throws Exception {
        Thread.sleep(60 * 1000 * 1000);
    }
}
