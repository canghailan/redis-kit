package cc.whohow.redis;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestRedisList2 {
    private static Redis redis;

    @BeforeClass
    public static void setup() throws Exception {
        redis = TestRedis.setupRedis();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        redis.close();
    }

}
