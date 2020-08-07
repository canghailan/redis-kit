package cc.whohow.redis;

import cc.whohow.redis.util.RedisLock;

import java.time.Duration;

public interface RedisLockFactory {
    RedisLock newLock(String key, Duration maxLockTime);

    RedisLock newLock(String key, Duration minLockTime, Duration maxLockTime);
}
