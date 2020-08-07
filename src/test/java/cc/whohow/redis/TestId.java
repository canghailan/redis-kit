package cc.whohow.redis;

import cc.whohow.redis.util.SnowflakeId;
import org.junit.Test;

public class TestId {
    @Test
    public void test() {
        System.out.println(System.currentTimeMillis());
        System.out.println(2018112014555500001L);

        SnowflakeId snowflakeId = new SnowflakeId.I52();

        for (int i = 0; i < 10; i++) {
            long id = snowflakeId.getAsLong();
            System.out.println("ID:\t\t" + id);
            System.out.println("时间:\t" + snowflakeId.extractDate(id));
            System.out.println("机器:\t" + snowflakeId.extractWorkerId(id));
            System.out.println("序列号:\t" + snowflakeId.extractSequence(id));
        }
    }
}
