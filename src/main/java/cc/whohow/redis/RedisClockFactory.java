package cc.whohow.redis;

import java.time.Clock;
import java.time.ZoneId;

public interface RedisClockFactory {
    default Clock clock() {
        return clock(ZoneId.systemDefault());
    }

    Clock clock(ZoneId zone);
}
