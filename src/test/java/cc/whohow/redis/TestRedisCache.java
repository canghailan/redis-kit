package cc.whohow.redis;

import cc.whohow.redis.client.RedisConnectionManagerAdapter;
import cc.whohow.redis.jcache.RedisCacheManager;
import cc.whohow.redis.jcache.configuration.MutableRedisCacheConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.client.codec.StringCodec;

import javax.cache.Cache;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestRedisCache {
    private static Redisson redisson;
    private static Redis redis;
    private static RedisCacheManager redisCacheManager;
    private static Cache<String, String> redisCache;
    private static Cache<String, String> redisExpireCache;

    @BeforeClass
    public static void setup() throws Exception {
        redisson = TestRedis.setupRedisson();
        redis = new RedisConnectionManagerAdapter(redisson.getConnectionManager());

        redisCacheManager = new RedisCacheManager(redis);

        MutableRedisCacheConfiguration<String, String> config1 = new MutableRedisCacheConfiguration<>();
        config1.setName("test");
        config1.setRedisCacheEnabled(true);
        config1.setKeyCodec(StringCodec.INSTANCE);
        config1.setValueCodec(StringCodec.INSTANCE);
        config1.setInProcessCacheEnabled(false);
        redisCache = redisCacheManager.createCache(config1.getName(), config1);

        MutableRedisCacheConfiguration<String, String> config2 = new MutableRedisCacheConfiguration<>();
        config2.setName("test-ex");
        config2.setRedisCacheEnabled(true);
        config2.setKeyCodec(StringCodec.INSTANCE);
        config2.setValueCodec(StringCodec.INSTANCE);
        config2.setExpiryForUpdate(60L);
        config2.setExpiryForUpdateTimeUnit(TimeUnit.SECONDS);
        config2.setInProcessCacheEnabled(false);
        redisExpireCache = redisCacheManager.createCache(config2.getName(), config2);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        redisCacheManager.close();
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
        Assert.assertEquals(map, redisCache.getAll(new HashSet<>(Arrays.asList("a", "b"))));
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
        redisCache.removeAll(new HashSet<>(Arrays.asList("a", "b", "c")));
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
