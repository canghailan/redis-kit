package cc.whohow.redis;

import cc.whohow.redis.io.JacksonCodec;
import cc.whohow.redis.io.UTF8Codec;
import cc.whohow.redis.lettuce.TimeOutput;
import cc.whohow.redis.util.RedisMap;
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
            redis = new SingleRedis(redisClient, redisURI);
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

    @Test
    public void testMap() {
        RedisMap<String, String> map = new RedisMap<>(redis,
                UTF8Codec.get(), new JacksonCodec<>(String.class), "test:map");

        map.clear();

        System.out.println(map.size());

        map.put("a", "aaa");
        map.put("b", "b");
        System.out.println(map.size());
        System.out.println(map.get("a"));
        System.out.println(map.get("b"));
        System.out.println(map.get("c"));
        System.out.println(map.copy());

        map.remove("a");
        System.out.println(map.size());
        System.out.println(map.get("a"));
        System.out.println(map.get("b"));
        System.out.println(map.get("c"));
        System.out.println(map.copy());
    }
}
