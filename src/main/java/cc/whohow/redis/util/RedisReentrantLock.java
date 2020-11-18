package cc.whohow.redis.util;

import cc.whohow.redis.Redis;

import java.time.Duration;
import java.util.concurrent.locks.ReentrantLock;

public class RedisReentrantLock extends RedisLock {
    protected final ReentrantLock lock = new ReentrantLock();

    public RedisReentrantLock(Redis redis, String key, Duration maxLockTime) {
        super(redis, key, maxLockTime);
    }

    public RedisReentrantLock(Redis redis, String key, Duration maxLockTime, String token) {
        super(redis, key, maxLockTime, token);
    }

    public RedisReentrantLock(Redis redis, String key, Duration minLockTime, Duration maxLockTime) {
        super(redis, key, minLockTime, maxLockTime);
    }

    public RedisReentrantLock(Redis redis, String key, Duration minLockTime, Duration maxLockTime, String token) {
        super(redis, key, minLockTime, maxLockTime, token);
    }

    @Override
    public boolean tryLock() {
        if (lock.tryLock()) {
            if (lock.getHoldCount() == 1) {
                try {
                    if (super.tryLock()) {
                        return true;
                    } else {
                        lock.unlock();
                        return false;
                    }
                } catch (Throwable e) {
                    lock.unlock();
                    throw e;
                }
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public void unlock() {
        try {
            if (lock.getHoldCount() == 1) {
                super.unlock();
            }
        } finally {
            lock.unlock();
        }
    }
}
