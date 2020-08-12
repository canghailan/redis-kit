package cc.whohow.redis;

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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRedisMessaging {
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
    public static void tearDown() throws Exception {
        connection.close();
        redisClient.shutdown();
    }

    @Test
    public void test() throws Exception {
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
}
