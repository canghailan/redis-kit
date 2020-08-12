package cc.whohow.redis;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.util.*;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TestRedisFactory {
    private static RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;
    private static StatefulRedisConnection<ByteBuffer, ByteBuffer> connection;
    private static RedisCommands<ByteBuffer, ByteBuffer> redis;
    private static RedisFactory redisFactory;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
            connection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
            redis = connection.sync();
            redisFactory = new RedisFactory(redisClient, redisURI);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        redisFactory.close();
        connection.close();
        redisClient.shutdown();
    }

    @Test
    public void testClock() throws Exception {
        Clock clock = redisFactory.clock();
        for (int i = 0; i < 10; i++) {
            Thread.sleep(ThreadLocalRandom.current().nextInt(3000));
            System.out.println(new Date(clock.millis()));
        }
    }

    @Test
    public void testIterator() {
        RedisAtomicLong c1 = new RedisAtomicLong(redis, "c:1");
        RedisAtomicLong c2 = new RedisAtomicLong(redis, "c:2");
        RedisAtomicLong c3 = new RedisAtomicLong(redis, "c:3");
        RedisAtomicLong c4 = new RedisAtomicLong(redis, "c:4");
        RedisAtomicLong c5 = new RedisAtomicLong(redis, "c:5");

        System.out.println(c5.get());

        c1.incrementAndGet();
        c1.incrementAndGet();
        c1.incrementAndGet();
        c2.incrementAndGet();
        c2.incrementAndGet();
        c3.incrementAndGet();
        c4.set(0);

        RedisKeyIterator iterator = new RedisKeyIterator(redis, "c:*");
        while (iterator.hasNext()) {
            String key = ByteBuffers.toUtf8String(iterator.next().duplicate());
            RedisAtomicLong c = new RedisAtomicLong(redis, key);
            System.out.println(key + " = " + c.get());
        }
    }

    @Test
    public void testRedisSortedSet() {
        RedisSortedSet<String> sortedSet = redisFactory.newSortedSet("testSortedSet", String.class);
        sortedSet.clear();

        Assert.assertEquals(0, sortedSet.size());

        sortedSet.put("a", 1);
        sortedSet.put("b", 5);
        sortedSet.put("c", 3);
        Assert.assertEquals(3, sortedSet.size());
        Assert.assertTrue(sortedSet.containsKey("c"));
        Assert.assertEquals(3.0, sortedSet.get("c"));
        Assert.assertTrue(sortedSet.containsValue(3.0));
        Assert.assertFalse(sortedSet.containsValue(4.0));

        System.out.println(sortedSet.copy());
    }

    @Test
    public void testRedisMap() {
        RedisMap<String, String> map = redisFactory.newMap("testMap", String.class, String.class);
        map.clear();

        Assert.assertEquals(0, map.size());

        map.put("a", "aa");
        map.put("b", "bb");
        map.put("c", "cc");
        Assert.assertEquals(3, map.size());
        Assert.assertTrue(map.containsKey("c"));
        Assert.assertEquals("cc", map.get("c"));

        Map<String, String> temp = new HashMap<>();
        temp.put("c", "c2");
        temp.put("d", "dd");
        temp.put("e", "ee");
        map.putAll(temp);
        Assert.assertEquals(5, map.size());
        Assert.assertTrue(map.containsKey("c"));
        Assert.assertTrue(map.containsKey("d"));
        Assert.assertEquals("c2", map.get("c"));

        map.remove("c");
        Assert.assertEquals(4, map.size());
        Assert.assertNull(map.get("c"));
        Assert.assertEquals(map.getOrDefault("c", "default"), "default");
        Assert.assertEquals(map.getOrDefault("d", "default"), "dd");

        map.putIfAbsent("d", "d2");
        Assert.assertEquals("dd", map.get("d"));

        System.out.println(map.copy());
    }

    @Test
    public void testTimeWindowCounter() throws Exception {
        RedisTimeWindowCounter counter = new RedisTimeWindowCounter(redis, "testTimeWindowCounter", Duration.ofSeconds(2));
        counter.reset();

        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            counter.incrementAndGet(new Date());
            Thread.sleep(random.nextInt(2500));
        }

        counter.copy().entrySet()
                .forEach(System.out::println);

        Assert.assertEquals(100, counter.sum());

        System.out.println(new Date());
        System.out.println(counter.sumLast(Duration.ofMinutes(1)));
        counter.retainLast(Duration.ofMinutes(1));
        System.out.println(counter.sum());

        counter.copy().entrySet()
                .forEach(System.out::println);
    }
}
