package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.time.Duration;
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
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final RedisScriptCommands redisScriptCommands;
    /**
     * 锁ID
     */
    protected final ByteBuffer key;
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
    protected final String random;

    public RedisLock(RedisCommands<ByteBuffer, ByteBuffer> redis, String key, Duration minLockTime, Duration maxLockTime) {
        this(redis, key, minLockTime, maxLockTime, UUID.randomUUID().toString());
    }

    public RedisLock(RedisCommands<ByteBuffer, ByteBuffer> redis, String key, Duration minLockTime, Duration maxLockTime, String random) {
        if (minLockTime.compareTo(maxLockTime) > 0) {
            throw new IllegalArgumentException();
        }
        this.redis = redis;
        this.redisScriptCommands = new RedisScriptCommands(redis);
        this.key = ByteBuffers.fromUtf8(key);
        this.minLockTimeMillis = minLockTime.toMillis();
        this.maxLockTimeMillis = maxLockTime.toMillis();
        this.random = random;
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
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(getRetryWaitingTime(retryTimes)));
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
        SetArgs setArgs = SetArgs.Builder.nx().px(maxLockTimeMillis);
        return Lettuce.ok(redis.set(key.duplicate(), ByteBuffers.fromUtf8(random), setArgs));
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
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(getRetryWaitingTime(retryTimes)));
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        return false;
    }

    /**
     * 解锁
     */
    @Override
    public void unlock() {
        redisScriptCommands.eval("cad", ScriptOutputType.INTEGER,
                new ByteBuffer[]{key.duplicate()}, ByteBuffers.fromUtf8(random));
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取重试等待时间，默认指数退避，直到最大锁定时间的一半
     */
    protected long getRetryWaitingTime(int retryTimes) {
        long time = (long) (minLockTimeMillis * Math.exp(retryTimes));
        return Math.min(time, maxLockTimeMillis / 2);
    }
}
