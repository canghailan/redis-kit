package cc.whohow.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class TestScript {
    private static final RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;
    private static Redis redis;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
            redis = new StandaloneRedis(redisClient, redisURI);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        redis.close();
        redisClient.shutdown();
    }

    @Test
    public void test() {
        RedisScript cas = RedisScript.get("cas");
        System.out.println(cas);
        System.out.println(cas.getName());
        System.out.println(cas.getSha1());
        System.out.println(cas.getScript());
        Assert.assertEquals("cas", cas.getName());
    }
}
