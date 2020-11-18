package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisScript;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.lettuce.IntegerOutput;
import cc.whohow.redis.lettuce.StatusOutput;
import io.lettuce.core.protocol.CommandType;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * Redis简易锁，需根据场景设计好参数，否则会导致异常情况（锁提前失效等）
 * https://redis.io/topics/distlock
 */
public class RedisLock implements Lock {
    private static final long AWAIT_TIME = 3000; // ms
    private static final long[] AWAIT_TIME_TABLE = {
            AWAIT_TIME, AWAIT_TIME, 2 * AWAIT_TIME, 3 * AWAIT_TIME,
            5 * AWAIT_TIME, 8 * AWAIT_TIME, 13 * AWAIT_TIME, 21 * AWAIT_TIME};
    protected final Redis redis;

    /**
     * 锁ID
     */
    protected final ByteSequence key;
    /**
     * 最小锁定时间，用于排除服务器时间误差，网络延时带来的影响，建议根据实际网络及服务器情况设置（大于网络延时及服务器时间差）
     */
    protected final long minLockTimeMillis;
    /**
     * 最大锁定时间，用于处理死锁，建议根据加锁任务最大耗时设置（大于最大耗时，小于任务间隔）
     */
    protected final long maxLockTimeMillis;
    /**
     * 锁秘钥，用于处理误解除非自己持有的锁
     */
    protected final ByteSequence token;

    public RedisLock(Redis redis, String key, Duration maxLockTime) {
        this(redis, key, Duration.ZERO, maxLockTime);
    }

    public RedisLock(Redis redis, String key, Duration maxLockTime, String token) {
        this(redis, key, Duration.ZERO, maxLockTime, token);
    }

    public RedisLock(Redis redis, String key, Duration minLockTime, Duration maxLockTime) {
        this(redis, key, minLockTime, maxLockTime, UUID.randomUUID().toString());
    }

    public RedisLock(Redis redis, String key, Duration minLockTime, Duration maxLockTime, String token) {
        if (minLockTime.compareTo(maxLockTime) > 0) {
            throw new IllegalArgumentException(minLockTime + "(minLockTime) > " + maxLockTime + "(maxLockTime)");
        }
        this.redis = redis;
        this.key = ByteSequence.utf8(key);
        this.minLockTimeMillis = minLockTime.toMillis();
        this.maxLockTimeMillis = maxLockTime.toMillis();
        this.token = ByteSequence.utf8(token);
    }

    /**
     * 阻塞重试，直到获得锁
     */
    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 阻塞重试，直到获得锁
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        for (int retryTimes = 0; ; retryTimes++) {
            if (tryLock()) {
                return;
            }
            await(retryTimes);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }

    /**
     * 尝试获得锁
     */
    @Override
    public boolean tryLock() {
        return RESP.ok(redis.send(new StatusOutput(),
                CommandType.SET, key, token, RESP.px(), RESP.b(maxLockTimeMillis), RESP.nx()));
    }

    /**
     * 尝试获得锁，直到超时
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long timeout = System.currentTimeMillis() + unit.toMillis(time);
        for (int retryTimes = 0; System.currentTimeMillis() <= timeout; retryTimes++) {
            if (tryLock()) {
                return true;
            }
            await(retryTimes);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        return false;
    }

    /**
     * 锁续期
     */
    public boolean renew(Duration duration) {
        return RESP.ok(redis.eval(new StatusOutput(),
                RedisScript.get("cas"),
                Collections.singletonList(key),
                Arrays.asList(token, token, RESP.px(), RESP.b(duration.toMillis()), RESP.xx())));
    }

    /**
     * 解锁
     *
     * @throws IllegalStateException unexpected token
     */
    @Override
    public void unlock() {
        long n = redis.eval(new IntegerOutput(),
                RedisScript.get("cad"),
                Collections.singletonList(key),
                Collections.singletonList(token));
        if (n == 0) {
            throw new IllegalStateException();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    protected void await(int retryTimes) {
        long time = getRetryWaitingTime(retryTimes);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(time));
    }

    /**
     * 获取重试等待时间
     */
    protected long getRetryWaitingTime(int retryTimes) {
        long time = (retryTimes < AWAIT_TIME_TABLE.length) ?
                AWAIT_TIME_TABLE[retryTimes] :
                AWAIT_TIME_TABLE[AWAIT_TIME_TABLE.length - 1];
        return Long.min(time, maxLockTimeMillis / 2);
    }

    @Override
    public String toString() {
        return key.toString();
    }
}
