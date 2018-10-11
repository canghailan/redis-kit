package cc.whohow.redis;

import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.util.RedisRateLimiter;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestRateLimiter {
    private static RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;
    private static StatefulRedisConnection<ByteBuffer, ByteBuffer> connection;
    private static RedisCommands<ByteBuffer, ByteBuffer> redis;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
            connection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
            redis = connection.sync();
        }
    }

    @AfterClass
    public static void tearDown() {
        connection.close();
        redisClient.shutdown();
    }

    @Test
    public void testRateLimiter() {
        RedisRateLimiter rateLimiter = new RedisRateLimiter(redis, "rateLimiter1", 10, Duration.ofMinutes(1L));
        for (int i = 0; i < 100; i++) {
            if (rateLimiter.tryAcquire(2, TimeUnit.MINUTES)) {
                System.out.println(new Date());
                System.out.println(i);
            }
        }
    }
}
