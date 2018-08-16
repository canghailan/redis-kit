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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 简易锁，需根据场景设计好参数，否则会导致异常情况（锁提前失效等）
 */
public class RedisLock implements Lock {
    private static final Pattern LINE = Pattern.compile("\n");
    private static final Pattern KEY_VALUE = Pattern.compile(": ");

    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    /**
     * 锁ID
     */
    protected final String id;
    protected final ByteBuffer encodedId;
    /**
     * 最小锁定时间，用于排除服务器时间误差，网络延时带来的影响，建议根据实际网络及服务器情况设置（大于网络延时及服务器时间差）
     */
    protected final Duration minLockTime;
    /**
     * 最大锁定时间，用于处理死锁，建议根据加锁任务最大耗时设置（大于最大耗时，小于任务间隔）
     */
    protected final Duration maxLockTime;
    /**
     * 锁秘钥，用于处理误解除非自己持有的锁
     */
    protected final String key = UUID.randomUUID().toString();
    /**
     * 线程ID，用于实现可重入锁
     */
    protected final String threadId = String.valueOf(Thread.currentThread().getId());

    public RedisLock(RedisCommands<ByteBuffer, ByteBuffer> redis, String id, Duration minLockTime, Duration maxLockTime) {
        if (minLockTime.compareTo(maxLockTime) > 0) {
            throw new IllegalArgumentException();
        }
        this.redis = redis;
        this.id = id;
        this.encodedId = ByteBuffers.fromUtf8(id);
        this.minLockTime = minLockTime;
        this.maxLockTime = maxLockTime;
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
        for(int retryTimes = 0; ; retryTimes++) {
            if (tryLock()) {
                return;
            }
            Thread.sleep(getRetryWaitingTime(retryTimes));
        }
    }

    /**
     * 尝试获得锁
     */
    @Override
    public boolean tryLock() {
        SetArgs setArgs = SetArgs.Builder.nx().px(maxLockTime.toMillis());
        CharSequence state = newState();
        return Lettuce.ok(redis.set(encodedId.duplicate(), ByteBuffers.fromUtf8(state), setArgs));
    }

    /**
     * 尝试获得锁，直到超时
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long timeout = System.currentTimeMillis() + unit.toMillis(time);
        for(int retryTimes = 0; System.currentTimeMillis() <= timeout ; retryTimes++) {
            if (tryLock()) {
                return true;
            }
            Thread.sleep(getRetryWaitingTime(retryTimes));
        }
        return false;
    }

    /**
     * 解锁
     */
    @Override
    public void unlock() {
        Map<String, String> state = readState();
        if (state == null) {
            throw new IllegalStateException("state == null");
        }
        String stateKey = state.get("key");
        if (!key.equals(stateKey)) {
            throw new IllegalStateException(key + " <> " + stateKey);
        }
        long lockTime = Long.parseLong(state.get("lockTime"));
        long lockedTime = System.currentTimeMillis() - lockTime;
        if (lockedTime < minLockTime.toMillis()) {
            // 小于最小锁定时间，调整过期时间
            long ttl = minLockTime.toMillis() - lockedTime;
            redis.pexpire(encodedId.duplicate(), ttl);
        } else if (lockedTime < maxLockTime.toMillis() - minLockTime.toMillis()) {
            // 小于(最大锁定时间-最小锁定时间)，删除
            redis.del(encodedId.duplicate());
        }
        // 接近最大锁定时间，等待过期
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取重试等待时间，默认指数退避，直到最大锁定时间的一半
     */
    protected long getRetryWaitingTime(int retryTimes) {
        long time = (long) (minLockTime.toMillis() * Math.exp(retryTimes));
        return Math.min(time, maxLockTime.toMillis() / 2);
    }

    /**
     * 生成最新锁状态，类YAML格式
     */
    protected CharSequence newState() {
        return new StringBuilder()
                .append("minLockTime: ").append(minLockTime).append('\n')
                .append("maxLockTime: ").append(maxLockTime).append('\n')
                .append("key: ").append(key).append('\n')
                .append("threadId: ").append(threadId).append('\n')
                .append("lockTime: ").append(System.currentTimeMillis()).append('\n');
    }

    /**
     * 读取Redis保存的锁状态
     */
    protected Map<String, String> readState() {
        ByteBuffer buffer = redis.get(encodedId.duplicate());
        if (buffer == null) {
            return null;
        }
        return parseState(StandardCharsets.UTF_8.decode(buffer));
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