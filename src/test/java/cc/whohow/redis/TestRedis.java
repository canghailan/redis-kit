package cc.whohow.redis;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.io.StringCodec;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.messaging.RedisMessaging;
import cc.whohow.redis.messaging.RedisPollingMessageQueue;
import cc.whohow.redis.util.*;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

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
//        RedisKey<String> redisKey = new RedisKey<>(redis, new StringCodec());
//        redisKey.put("a", "1");
//        redisKey.put("b", "2");
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

        String prefix = "c:";

        RedisKeyIterator iterator = new RedisKeyIterator(redis, prefix + "*");
        while (iterator.hasNext()) {
            String key = ByteBuffers.toUtf8String(iterator.next());
            RedisAtomicLong c = new RedisAtomicLong(redis, key);
            long value = c.getAndSet(0);
            System.out.println("aaa" + key.substring(prefix.length()) + " = " + value);
        }
    }

    @Test
    public void testKeyspaceEvent() throws Exception {
        RedisKeyspaceNotification redisKeyspaceNotification = new RedisKeyspaceNotification(redisClient, redisURI);

        Thread.sleep(300_000);
    }

    @Test
    public void testMessageQueue() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        RedisKeyspaceNotification redisKeyspaceNotification = new RedisKeyspaceNotification(redisClient, redisURI);

        RedisMessaging redisMessaging = new RedisMessaging(redis, redisKeyspaceNotification, executor);

        AtomicInteger counter = new AtomicInteger(0);

        List<RedisPollingMessageQueue<String>> mqs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String name = "mq" + i;
            RedisPollingMessageQueue<String> mq = redisMessaging.createQueue(name, new StringCodec(),
                    (m) -> {
                        try {
                            long ms = ThreadLocalRandom.current().nextLong(5_000);
                            Thread.sleep(ms);
                            System.out.println(Thread.currentThread() + " 消费 " + name + ": " + m + "  " + ms + "ms");
                            System.out.println("counter: " + counter.incrementAndGet());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
            mq.setReady();
            mqs.add(mq);
        }

        for (int i = 0; i < 30; i++) {
            executor.submit(() -> {
                int index = ThreadLocalRandom.current().nextInt(mqs.size());
                RedisPollingMessageQueue<String> mq = mqs.get(index);
                String data = mq.getName() + "_" + ThreadLocalRandom.current().nextInt(100);
                mq.offer(data);
                System.out.println(Thread.currentThread() + " 生产 " + mq.getName() + ": " + data);
            });
            Thread.sleep(ThreadLocalRandom.current().nextLong(1_000));
        }

        Thread.sleep(300_000);
    }

    @Test
    public void test() {
        RedisMap<String, String> map = new RedisMap<>(redis, new StringCodec(), new StringCodec(), "testMap");
        map.put("a", "aa");
        map.put("b", "bb");
        map.put("c", "cc");
        System.out.println(map.get());
        map.remove("c");
        System.out.println(map.get());

        RedisSortedSet<String> sortedSet = new RedisSortedSet<>(redis, new StringCodec(), "testSortedSet");
        sortedSet.put("a", 1);
        sortedSet.put("b", 5);
        sortedSet.put("c", 3);
        sortedSet.put("d", 2);
        System.out.println(sortedSet.get());

        for (Map.Entry<String, Number> e : sortedSet.entrySet()) {
            System.out.println(e.getKey() + "=" + e.getValue());
        }

        RedisKeyIterator redisKeyIterator = new RedisKeyIterator(redis);
        while (redisKeyIterator.hasNext()) {
            System.out.println(ByteBuffers.toUtf8String(redisKeyIterator.next()));
        }
    }

    @Test
    public void testTimeWindowCounter() throws Exception {
        RedisTimeWindowCounter counter = new RedisTimeWindowCounter(redis, "wt", Duration.ofSeconds(2));
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            Date date = new Date();
            counter.incrementAndGet(date);
            System.out.println(date);
            Thread.sleep(random.nextInt(2500));
        }

        System.out.println(counter.sumLast(Duration.ofMinutes(1)));
        System.out.println(counter.sum());
        counter.get().entrySet()
                .forEach(System.out::println);
        counter.retainLast(Duration.ofMinutes(1));
        counter.get().entrySet()
                .forEach(System.out::println);
    }
}
