package cc.whohow.redis;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.StringCodec;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.util.RedisClock;
import cc.whohow.redis.util.RedisKey;
import cc.whohow.redis.util.RedisKeyIterator;
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
import java.util.Properties;

public class TestRedis {
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
    public void testClock() {
        RedisClock clock = new RedisClock(redis);
        for (int i = 0; i < 10; i++) {
            System.out.println(clock.millis());
        }
    }

    @Test
    public void testIterator() {
        RedisKey<String> redisKey = new RedisKey<>(redis, new StringCodec());
        redisKey.put("a", "1");
        redisKey.put("b", "2");

        RedisKeyIterator iterator = new RedisKeyIterator(redis);
        while (iterator.hasNext()) {
            System.out.println(ByteBuffers.toUtf8String(iterator.next()));
        }
    }
}
