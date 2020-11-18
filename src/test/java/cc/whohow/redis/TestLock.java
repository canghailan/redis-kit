package cc.whohow.redis;

import cc.whohow.redis.util.RedisLock;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class TestLock {
    private static final RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;
    private static Redis redis;
    private static ExecutorService executor;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
            redis = new LoggingRedis(new StandaloneRedis(redisClient, redisURI));

            executor = Executors.newFixedThreadPool(20);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        executor.shutdownNow();
        redis.close();
        redisClient.shutdown();
    }

    @Test
    public void testLock() throws Exception {
        RedisLock lock1 = new RedisLock(redis, "lock:test", Duration.ofMinutes(1), Duration.ofMinutes(30));
        RedisLock lock2 = new RedisLock(redis, "lock:test", Duration.ofMinutes(1), Duration.ofMinutes(30));
        for (int i = 0; i < 10; i++) {
            executor.submit(new Task1(lock2));
        }
        for (int i = 0; i < 10; i++) {
            executor.submit(new Task1(lock1));
        }
        executor.submit(new Task2(lock1));
        executor.submit(new Task2(lock1));
        executor.submit(new Task2(lock1));
        executor.awaitTermination(10, TimeUnit.MINUTES);
    }

    private static class Task1 implements Runnable {
        private final Lock lock;

        private Task1(Lock lock) {
            this.lock = lock;
        }

        @Override
        public void run() {
            Thread thread = Thread.currentThread();
            if (lock.tryLock()) {
                System.out.println(lock + " LOCKED  \t" + thread.getName());
                try {
                    Thread.sleep(5000L);
                } catch (Throwable e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                    System.out.println(lock + " UNLOCK  \t" + thread.getName());
                }
            } else {
                System.out.println(lock + " FAILED  \t" + thread.getName());
            }
        }
    }

    private static class Task2 implements Runnable {
        private final Lock lock;

        private Task2(Lock lock) {
            this.lock = lock;
        }

        @Override
        public void run() {
            Thread thread = Thread.currentThread();
            System.out.println(lock + " LOCKING \t" + thread.getName());
            lock.lock();
            System.out.println(lock + " LOCKED  \t" + thread.getName());
            try {
                Thread.sleep(5000L);
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
                System.out.println(lock + " UNLOCK  \t" + thread.getName());
            }
        }
    }
}
