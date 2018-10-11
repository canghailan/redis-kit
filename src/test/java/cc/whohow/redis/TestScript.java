package cc.whohow.redis;

import cc.whohow.redis.script.RedisScript;
import org.junit.Test;

public class TestScript {
    @Test
    public void testScript() {
        RedisScript script = new RedisScript("pincrbyex");
        System.out.println(script.getScript());
        System.out.println(script.getSha1());
    }
}
