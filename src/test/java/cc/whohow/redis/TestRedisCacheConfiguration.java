package cc.whohow.redis;

import org.junit.Test;

import javax.cache.annotation.CacheKey;
import java.util.Arrays;
import java.util.List;

public class TestRedisCacheConfiguration {
    public List<String> getList(Long id) {
        return null;
    }

    public List<String> getList(Long id, String name) {
        return null;
    }

    public List<String> getList(Long id, List<String> names) {
        return null;
    }

    public List<String> getList(@CacheKey int key, Long id, List<String> names) {
        return null;
    }

    @Test
    public void testCacheMethods() throws Exception {
        System.out.println(CacheMethods.getValueTypeCanonicalName(
                TestRedisCacheConfiguration.class.getMethod("getList", Long.class)));
        System.out.println(Arrays.toString(CacheMethods.getKeyTypeCanonicalName(
                TestRedisCacheConfiguration.class.getMethod("getList", Long.class))));
        System.out.println(Arrays.toString(CacheMethods.getKeyTypeCanonicalName(
                TestRedisCacheConfiguration.class.getMethod("getList", Long.class, String.class))));
        System.out.println(Arrays.toString(CacheMethods.getKeyTypeCanonicalName(
                TestRedisCacheConfiguration.class.getMethod("getList", Long.class, List.class))));
        System.out.println(Arrays.toString(CacheMethods.getKeyTypeCanonicalName(
                TestRedisCacheConfiguration.class.getMethod("getList", int.class, Long.class, List.class))));
    }
}
