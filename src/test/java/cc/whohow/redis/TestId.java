package cc.whohow.redis;

import cc.whohow.redis.distributed.SnowflakeId;
import cc.whohow.redis.distributed.SnowflakeId52;
import org.junit.Test;

public class TestId {
    @Test
    public void test() {
        System.out.println(System.currentTimeMillis());
        System.out.println(2018112014555500001L);

        SnowflakeId snowflakeId = new SnowflakeId52();

        for (int i = 0; i < 10; i++) {
            long id = snowflakeId.getAsLong();
            System.out.println(snowflakeId.random(System.currentTimeMillis()));
            System.out.println(2020000000000000000L + id);
        }
    }
}
