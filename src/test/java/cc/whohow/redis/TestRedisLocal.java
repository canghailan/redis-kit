package cc.whohow.redis;

import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.util.RedisLocal;
import cc.whohow.redis.util.SnowflakeId;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TestRedisLocal {
    private static final RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;
    private static StatefulRedisConnection<ByteBuffer, ByteBuffer> connection;
    private static RedisCommands<ByteBuffer, ByteBuffer> redis;
    private static ScheduledExecutorService executor;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
            connection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
            redis = connection.sync();
        }
        executor = Executors.newScheduledThreadPool(1);
    }

    @AfterClass
    public static void tearDown() {
        connection.close();
        redisClient.shutdown();
        executor.shutdownNow();
    }

    @Test
    public void test() throws Exception {
        RedisLocal redisLocal = new RedisLocal(redis, executor, "RL");
        System.out.println(redisLocal.getId());
        System.out.println(redisLocal.getActiveIds());
        System.out.println(redisLocal.isLeader());
        System.out.println(redisLocal.getLocalMap().orElseThrow(IllegalStateException::new).copy());
        Thread.sleep(10_000);
        System.out.println(redisLocal.getId());
        System.out.println(redisLocal.getActiveIds());
        System.out.println(redisLocal.isLeader());
        System.out.println(redisLocal.getLocalMap().orElseThrow(IllegalStateException::new).copy());
        Thread.sleep(600_000);
    }

    @Test
    public void testSnowflakeId() {
        SnowflakeId snowflakeId = new SnowflakeId.I52();
        for (int i = 0; i < 10; i++) {
            long id = snowflakeId.getAsLong();
            System.out.println("ID:\t\t" + id);
            System.out.println("时间:\t" + snowflakeId.extractDate(id));
            System.out.println("机器:\t" + snowflakeId.extractWorkerId(id));
            System.out.println("序列号:\t" + snowflakeId.extractSequence(id));
            System.out.println();
        }
    }

    @Test
    public void testRedisSnowflakeId() {
        SnowflakeId snowflakeId = new RedisSnowflakeIdFactory(
                new RedisLocal(redis, executor, "RL")).newI52Generator();

        for (int i = 0; i < 10; i++) {
            long id = snowflakeId.getAsLong();
            System.out.println("ID:\t\t" + id);
            System.out.println("时间:\t" + snowflakeId.extractDate(id));
            System.out.println("机器:\t" + snowflakeId.extractWorkerId(id));
            System.out.println("序列号:\t" + snowflakeId.extractSequence(id));
            System.out.println();
        }
    }
}
