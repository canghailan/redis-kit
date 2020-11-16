package cc.whohow.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class TestRedisMessaging {
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
            redis = new StandaloneRedis(redisClient, redisURI);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        redis.close();
        redisClient.shutdown();
    }

//    @Test
//    public void test() throws Exception {
//        ExecutorService executor = Executors.newCachedThreadPool();
//        RedisKeyspaceNotification redisKeyspaceNotification = new RedisKeyspaceNotification(redisClient, redisURI);
//
//        RedisMessaging redisMessaging = new RedisMessaging(redis, redisKeyspaceNotification, executor);
//
//        AtomicInteger counter = new AtomicInteger(0);
//
//        List<RedisPollingMessageQueue<String>> mqs = new ArrayList<>();
//        for (int i = 0; i < 10; i++) {
//            String name = "mq" + i;
//            RedisPollingMessageQueue<String> mq = redisMessaging.createQueue(name, new StringCodec(),
//                    (m) -> {
//                        try {
//                            long ms = ThreadLocalRandom.current().nextLong(5_000);
//                            Thread.sleep(ms);
//                            System.out.println(Thread.currentThread() + " 消费 " + name + ": " + m + "  " + ms + "ms");
//                            System.out.println("counter: " + counter.incrementAndGet());
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    });
//            mq.setReady();
//            mqs.add(mq);
//        }
//
//        for (int i = 0; i < 30; i++) {
//            executor.submit(() -> {
//                int index = ThreadLocalRandom.current().nextInt(mqs.size());
//                RedisPollingMessageQueue<String> mq = mqs.get(index);
//                String data = mq.getName() + "_" + ThreadLocalRandom.current().nextInt(100);
//                mq.offer(data);
//                System.out.println(Thread.currentThread() + " 生产 " + mq.getName() + ": " + data);
//            });
//            Thread.sleep(ThreadLocalRandom.current().nextLong(1_000));
//        }
//
//        Thread.sleep(300_000);
//    }
}
