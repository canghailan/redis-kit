package cc.whohow.redis;

import cc.whohow.redis.util.*;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TestRedisFactory {
    private static final RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;
    private static Redis redis;
    private static RedisFactory redisFactory;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
            redis = new LoggingRedis(new StandaloneRedis(redisClient, redisURI));
            redisFactory = new RedisFactory(redis);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        redisFactory.close();
        redis.close();
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
    public void testAtomicLong() {
        RedisAtomicLong n = redisFactory.newAtomicLong("atomicLong");

        n.set(1);
        Assert.assertEquals(1, n.get());

        Assert.assertEquals(1, n.getAndSet(2));
        Assert.assertEquals(2, n.get());
        Assert.assertEquals(2, n.getAndIncrement());
        Assert.assertEquals(3, n.getAndAdd(2));
        Assert.assertEquals(5, n.getAndAdd(-2));
        Assert.assertEquals(3, n.getAndDecrement());
        Assert.assertEquals(1, n.decrementAndGet());
        Assert.assertEquals(2, n.incrementAndGet());
        Assert.assertEquals(4, n.addAndGet(2));
    }

    @Test
    public void testAtomicReference() throws Exception {
        RedisAtomicReference<String> ref = redisFactory.newAtomicReference("atomicRef", String.class);

        ref.reset();
        Assert.assertFalse(ref.exists());

        ref.set("a");
        Assert.assertTrue(ref.exists());
        Assert.assertEquals("a", ref.get());

        ref.compareAndSet("b", "bb");
        Assert.assertEquals("a", ref.get());

        ref.compareAndSet("a", "b");
        Assert.assertEquals("b", ref.get());

        ref.compareAndReset("bb");
        Assert.assertEquals("b", ref.get());

        ref.compareAndReset("b");
        Assert.assertNull(ref.get());

        ref.setIfAbsent("c");
        Assert.assertEquals("c", ref.get());

        ref.setIfAbsent("d");
        Assert.assertEquals("c", ref.get());

        ref.setIfPresent("d");
        Assert.assertEquals("d", ref.get());

        Assert.assertEquals("d", ref.getAndSet("e"));
        Assert.assertEquals("e", ref.get());
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

        Iterator<String> iterator = new RedisIterator<>(new RedisKeyScanIterator<>(redis, RESP::ascii, "c:*", 0));
        while (iterator.hasNext()) {
            String key = iterator.next();
            RedisAtomicLong c = new RedisAtomicLong(redis, key);
            System.out.println(key + " = " + c.get());
        }
    }

    @Test
    public void testRedisList() {
        RedisList<String> list = redisFactory.newList("list", String.class);
        list.clear();

        Assert.assertEquals(0, list.size());

        list.add("a");
        list.add("b");
        list.add("c");
        Assert.assertEquals(3, list.size());
        Assert.assertEquals("c", list.get(2));

        list.set(2, "cc");
        Assert.assertEquals(3, list.size());
        Assert.assertEquals("cc", list.get(2));

        List<String> temp = new ArrayList<>();
        temp.add("c");
        temp.add("d");
        temp.add("e");
        list.addAll(temp);
        Assert.assertEquals(6, list.size());
        Assert.assertEquals("c", list.get(3));

        list.remove("c");
        Assert.assertEquals(5, list.size());

        Assert.assertEquals("a", list.peekFirst());
        Assert.assertEquals(5, list.size());

        Assert.assertEquals("e", list.peekLast());
        Assert.assertEquals(5, list.size());

        Assert.assertEquals("a", list.pollFirst());
        Assert.assertEquals(4, list.size());

        Assert.assertEquals("e", list.pollLast());
        Assert.assertEquals(3, list.size());

        list.addFirst("aa");
        Assert.assertEquals("aa", list.peekFirst());

        list.addLast("ee");
        Assert.assertEquals("ee", list.peekLast());

        System.out.println(list.copy());
    }

    @Test
    public void testRedisSet() {
        RedisSet<String> set = redisFactory.newSet("set", String.class);
        set.clear();

        Assert.assertEquals(0, set.size());

        set.add("a");
        set.add("b");
        set.add("c");
        Assert.assertEquals(3, set.size());
        Assert.assertTrue(set.contains("c"));

        List<String> temp = new ArrayList<>();
        temp.add("c");
        temp.add("d");
        temp.add("e");
        set.addAll(temp);
        Assert.assertEquals(5, set.size());
        Assert.assertTrue(set.contains("c"));

        set.remove("c");
        Assert.assertEquals(4, set.size());
        Assert.assertFalse(set.contains("c"));

        Assert.assertNotNull(set.peek());
        Assert.assertEquals(4, set.size());

        Assert.assertNotNull(set.poll());
        Assert.assertEquals(3, set.size());

        System.out.println(set.copy());
    }

    @Test
    public void testRedisSortedSet() {
        RedisSortedSet<String> sortedSet = redisFactory.newSortedSet("zset", String.class);
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

        Map<String, Number> temp = new HashMap<>();
        temp.put("c", 4);
        temp.put("d", 5);
        temp.put("e", 6);
        sortedSet.putAll(temp);
        Assert.assertEquals(5, sortedSet.size());
        Assert.assertEquals(4.0, sortedSet.get("c"));

        sortedSet.remove("c");
        Assert.assertEquals(4, sortedSet.size());
        Assert.assertNull(sortedSet.get("c"));

        sortedSet.putIfAbsent("d", 6);
        Assert.assertEquals(5.0, sortedSet.get("d"));

        sortedSet.replace("e", 7);
        Assert.assertEquals(7.0, sortedSet.get("e"));

        System.out.println(sortedSet.copy());
    }

    @Test
    public void testRedisDelayQueue() {
        RedisDelayQueue<String> delayQueue = redisFactory.newDelayQueue("delayQueue", String.class);
        delayQueue.clear();

        Assert.assertEquals(0, delayQueue.size());

        delayQueue.add("a", new Date());
        delayQueue.add("b", new Date());
        delayQueue.add("c", new Date());
        Assert.assertEquals(3, delayQueue.size());
        Assert.assertTrue(delayQueue.containsElement("a"));
        Assert.assertFalse(delayQueue.containsElement("d"));

        Assert.assertFalse(delayQueue.isEmpty());
        Assert.assertEquals("a", delayQueue.poll().getKey());
        Assert.assertFalse(delayQueue.isEmpty());
        Assert.assertEquals("b", delayQueue.poll().getKey());
        Assert.assertFalse(delayQueue.isEmpty());
        Assert.assertEquals("c", delayQueue.poll().getKey());

        delayQueue.add("d", Duration.ofMinutes(3));
        Assert.assertNull(delayQueue.poll());

        System.out.println(delayQueue.copy());
    }

    @Test
    public void testRedisMap() {
        RedisMap<String, String> map = redisFactory.newMap("map:test", String.class, String.class);
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
        RedisTimeWindowCounter counter = new RedisTimeWindowCounter(redis, "TimeWindowCounter:test", Duration.ofSeconds(2));
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
        long sumLast = counter.sumLast(Duration.ofMinutes(1));
        counter.retainLast(Duration.ofMinutes(1));
        Assert.assertEquals(sumLast, counter.sum());

        System.out.println(sumLast);
        counter.copy().entrySet()
                .forEach(System.out::println);
    }
}
