package cc.whohow.redis;

import cc.whohow.redis.util.impl.DateRange;
import org.junit.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;

public class TestDateRange {
    @Test
    public void testRange() {
        DateRange dates = new DateRange(
                ZonedDateTime.now().withMinute(0).withSecond(0).withNano(0),
                ZonedDateTime.now().withMinute(0).withSecond(0).withNano(0).plusHours(1),
                Duration.ofMinutes(5)
        );

        for (Date date : dates) {
            System.out.println(date);
        }
    }
}
