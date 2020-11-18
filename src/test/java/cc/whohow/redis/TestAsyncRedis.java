package cc.whohow.redis;

import cc.whohow.redis.lettuce.TimeOutput;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.CommandType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestAsyncRedis {
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
            redis = new LoggingRedis(new StandaloneRedis(redisClient, redisURI));
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        redis.close();
        redisClient.shutdown();
    }

    @Test
    public void testTime() {
        long time = redis.sendAsync(new TimeOutput(), CommandType.TIME).join();
        System.out.println(time);
        System.out.println(new Date(TimeUnit.MICROSECONDS.toMillis(time)));
    }
}
