package cc.whohow.redis;

import cc.whohow.redis.jcache.RedisCache;
import cc.whohow.redis.jcache.RedisExpireCache;
import cc.whohow.redis.client.ConnectionPoolRedis;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class TestRedisCache {
    private Redis redis;
    private RedisCache<String, String> redisCache;
    private RedisExpireCache<String, String> redisExpireCache;

    @Before
    public void setup() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            Properties properties = new Properties();
            properties.load(stream);

            String host = properties.getProperty("host");
            int port = Integer.parseInt(properties.getProperty("port", "6379"));
            String password = properties.getProperty("password", "");
            int database = Integer.parseInt(properties.getProperty("database", "0"));

            Config config = new Config();
            config.useSingleServer()
                    .setAddress("redis://" + host + ":" + port)
                    .setPassword(password)
                    .setDatabase(database);
            redis = new ConnectionPoolRedis(config);

            redisCache = new RedisCache<>("test", redis, StringCodec.INSTANCE, StringCodec.INSTANCE);
            redisExpireCache = new RedisExpireCache<>("test_ex", redis, StringCodec.INSTANCE, StringCodec.INSTANCE, 60_000);
        }
    }

    @After
    public void tearDown() throws Exception {
        redis.close();
    }

    @Test
    public void testPutGet() {
        redisCache.put("a", "z");
        Assert.assertEquals("z", redisCache.get("a"));
    }

    @Test
    public void testGetAll() {
        Map<String, String> map = new HashMap<>();
        map.put("a", "z");
        map.put("b", "y");

        redisCache.put("a", "z");
        redisCache.put("b", "y");
        redisCache.put("c", "x");
        Assert.assertEquals(map, redisCache.getAll(new HashSet<>(Arrays.asList("a","b"))));
    }

    @Test
    public void testContainsKey() {
        redisCache.put("a", "z");
        Assert.assertTrue(redisCache.containsKey("a"));

        redisCache.remove("aa");
        Assert.assertFalse(redisCache.containsKey("aa"));
    }

    @Test
    public void testGetAndPut() {
        redisCache.put("a", "z");
        Assert.assertEquals("z", redisCache.getAndPut("a", "az"));
        Assert.assertEquals("az", redisCache.get("a"));
    }

    @Test
    public void testPutAll() {
        Map<String, String> map = new HashMap<>();
        map.put("q", "qq");
        map.put("w", "ww");
        map.put("e", "ee");
        map.put("r", "rr");
        redisCache.putAll(map);
        Assert.assertEquals(map, redisCache.getAll(map.keySet()));
    }

    @Test
    public void testPutIfAbsent() {
        redisCache.put("a", "z");
        Assert.assertEquals("z", redisCache.get("a"));
        Assert.assertFalse(redisCache.putIfAbsent("a", "abc"));
        Assert.assertEquals("z", redisCache.get("a"));

        redisCache.remove("m");
        Assert.assertTrue(redisCache.putIfAbsent("m", "abc"));
        Assert.assertEquals("abc", redisCache.get("m"));
    }

    @Test
    public void testReplace() {
        redisCache.put("a", "z");
        Assert.assertEquals("z", redisCache.get("a"));
        Assert.assertTrue(redisCache.replace("a", "abc"));
        Assert.assertEquals("abc", redisCache.get("a"));

        redisCache.remove("m");
        Assert.assertFalse(redisCache.replace("m", "abc"));
    }

    @Test
    public void testRemove() {
        redisCache.remove("n");
        Assert.assertFalse(redisCache.remove("n"));

        redisCache.put("m", "m");
        Assert.assertTrue(redisCache.remove("m"));
    }

    @Test
    public void testRemoveAll() {
        redisCache.put("a", "z");
        Assert.assertTrue(redisCache.containsKey("a"));
        redisCache.remove("c");
        redisCache.removeAll(new HashSet<>(Arrays.asList("a","b","c")));
        Assert.assertFalse(redisCache.containsKey("a"));
        Assert.assertFalse(redisCache.containsKey("b"));
        Assert.assertFalse(redisCache.containsKey("c"));
    }

    @Test
    public void testClear() {
        redisCache.put("a", "z");
        redisCache.put("b", "y");
        Assert.assertTrue(redisCache.containsKey("a"));
        Assert.assertTrue(redisCache.containsKey("b"));
        redisCache.remove("c");
        redisCache.removeAll();
        Assert.assertFalse(redisCache.containsKey("a"));
        Assert.assertFalse(redisCache.containsKey("b"));
        Assert.assertFalse(redisCache.containsKey("c"));
    }

    @Test
    public void testPutGetExpire() {
        redisExpireCache.put("a", "z");
        Assert.assertEquals("z", redisExpireCache.get("a"));
    }

    @Test
    public void testPutIfAbsentExpire() {
        redisExpireCache.remove("m");
        Assert.assertTrue(redisExpireCache.putIfAbsent("m", "abc"));
        Assert.assertEquals("abc", redisExpireCache.get("m"));

        redisExpireCache.put("a", "z");
        Assert.assertEquals("z", redisExpireCache.get("a"));
        Assert.assertFalse(redisExpireCache.putIfAbsent("a", "abc"));
        Assert.assertEquals("z", redisExpireCache.get("a"));
    }

    @Test
    public void testReplaceExpire() {
        redisExpireCache.remove("m");
        Assert.assertFalse(redisExpireCache.replace("m", "abc"));

        redisExpireCache.put("a", "z");
        Assert.assertEquals("z", redisExpireCache.get("a"));
        Assert.assertTrue(redisExpireCache.replace("a", "abc"));
        Assert.assertEquals("abc", redisExpireCache.get("a"));
    }
}
