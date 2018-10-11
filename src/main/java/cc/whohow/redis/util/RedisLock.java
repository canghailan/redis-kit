package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.lettuce.Lettuce;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Redis简易锁，需根据场景设计好参数，否则会导致异常情况（锁提前失效等）
 */
public class RedisLock implements Lock {
    private static final Pattern LINE = Pattern.compile("\n");
    private static final Pattern KEY_VALUE = Pattern.compile(": ");

    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
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
    protected final String ownerKey;

    public RedisLock(RedisCommands<ByteBuffer, ByteBuffer> redis, String key, Duration minLockTime, Duration maxLockTime) {
        if (minLockTime.compareTo(maxLockTime) > 0) {
            throw new IllegalArgumentException();
        }
        this.redis = redis;
        this.key = ByteBuffers.fromUtf8(key);
        this.minLockTimeMillis = minLockTime.toMillis();
        this.maxLockTimeMillis = maxLockTime.toMillis();
        this.ownerKey = newOwnerKey();
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
        CharSequence state = newState();
        return Lettuce.ok(redis.set(key.duplicate(), ByteBuffers.fromUtf8(state), setArgs));
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
        Map<String, String> state = getState();
        if (state == null) {
            throw new IllegalStateException("state == null");
        }
        String stateKey = state.get("key");
        if (!ownerKey.equals(stateKey)) {
            // key不相同表示这不是自己加的锁
            throw new IllegalStateException(ownerKey + " <> " + stateKey);
        }
        long lockTime = Long.parseLong(state.get("lockTime"));
        // 已锁定时间
        long lockedTime = System.currentTimeMillis() - lockTime;
        if (lockedTime < minLockTimeMillis) {
            // 小于最小锁定时间，调整过期时间
            long ttl = minLockTimeMillis - lockedTime;
            redis.pexpire(key.duplicate(), ttl);
        } else if (lockedTime < maxLockTimeMillis - minLockTimeMillis) {
            // 小于(最大锁定时间-最小锁定时间)，删除
            redis.del(key.duplicate());
        }
        // 接近最大锁定时间，等待过期
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    /**
     * 读取Redis保存的锁状态
     */
    public Map<String, String> getState() {
        ByteBuffer buffer = redis.get(key.duplicate());
        if (buffer == null) {
            return null;
        }
        return parseState(StandardCharsets.UTF_8.decode(buffer));
    }

    /**
     * 获取重试等待时间，默认指数退避，直到最大锁定时间的一半
     */
    protected long getRetryWaitingTime(int retryTimes) {
        long time = (long) (minLockTimeMillis * Math.exp(retryTimes));
        return Math.min(time, maxLockTimeMillis / 2);
    }

    /**
     * 生成一个新的Key
     */
    protected String newOwnerKey() {
        return UUID.randomUUID().toString();
    }

    /**
     * 生成最新锁状态，类YAML格式
     */
    protected CharSequence newState() {
        return new StringBuilder()
                .append("minLockTime: ").append(minLockTimeMillis).append('\n')
                .append("maxLockTime: ").append(maxLockTimeMillis).append('\n')
                .append("key: ").append(ownerKey).append('\n')
                // 线程ID，用于实现可重入锁
                .append("threadId: ").append(Thread.currentThread().getId()).append('\n')
                // 锁定时间，用于处理解锁
                .append("lockTime: ").append(System.currentTimeMillis()).append('\n');
    }

    /**
     * 解析锁状态
     */
    protected Map<String, String> parseState(CharSequence state) {
        return LINE.splitAsStream(state)
                .filter(line -> !line.isEmpty())
                .map(line -> KEY_VALUE.split(line, 2))
                .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }
}
