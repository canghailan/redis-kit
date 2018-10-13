package cc.whohow.redis;

import cc.whohow.redis.distributed.RedisDistributed;
import cc.whohow.redis.distributed.RedisDistributedIdGenerator;
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
    public void test() throws Exception {
        try (RedisDistributed distributed = new RedisDistributed(redisClient, redisURI, 256)) {
            distributed.run();
            System.out.println(distributed.getId());
            System.out.println(distributed.getNodeIdSet());
            System.out.println(distributed.getLeaderId());
            System.out.println(distributed.gc());
        }
    }

    @Test
    public void testIdGenerator() {
        try (RedisDistributed distributed = new RedisDistributed(redisClient, redisURI, 256)) {
            distributed.run();
            distributed.gc();
            System.out.println(distributed.getId());

            RedisDistributedIdGenerator idGenerator = new RedisDistributedIdGenerator(distributed);
            long timestamp = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                System.out.println(idGenerator.getAsLong());
            }
            long time = System.currentTimeMillis() - timestamp;
            System.out.println(time + " ms");
        }
    }
}
