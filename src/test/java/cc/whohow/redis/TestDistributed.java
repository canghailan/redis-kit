package cc.whohow.redis;

import cc.whohow.redis.distributed.RedisDistributed;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class TestDistributed {
    private static RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
        }
    }

    @AfterClass
    public static void tearDown() {
        redisClient.shutdown();
    }

    @Test
    public void testDistributed() throws Exception {
        try (RedisDistributed distributed = new RedisDistributed(redisClient, redisURI)) {
            System.out.println(distributed.getUuid());
            System.out.println(distributed.getNodeUuidSet());
        }
    }
}
