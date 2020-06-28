package cc.whohow.redis.spring.scheduling;

import cc.whohow.redis.spring.annotation.RedisScheduled;
import cc.whohow.redis.util.LockRunnable;
import cc.whohow.redis.util.RedisLock;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.scheduling.config.Task;
import org.springframework.scheduling.support.ScheduledMethodRunnable;

import java.nio.ByteBuffer;
import java.time.Duration;

public class RedisScheduledTask extends Task implements Runnable {
    private final RedisCommands<ByteBuffer, ByteBuffer> redis;
    private final Runnable delegate;

    public RedisScheduledTask(Runnable runnable, RedisCommands<ByteBuffer, ByteBuffer> redis) {
        super(runnable);
        this.redis = redis;
        this.delegate = proxy(runnable);
    }

    private Runnable proxy(Runnable runnable) {
        if (runnable instanceof ScheduledMethodRunnable) {
            ScheduledMethodRunnable scheduledMethodRunnable = (ScheduledMethodRunnable) runnable;
            RedisScheduled redisScheduled = scheduledMethodRunnable.getMethod().getAnnotation(RedisScheduled.class);
            if (redisScheduled != null) {
                return proxy(runnable, redisScheduled);
            }
        }
        return runnable;
    }

    private Runnable proxy(Runnable runnable, RedisScheduled redisScheduled) {
//        if (redisScheduled.retry() > 0) {
//            runnable = new RetryRunnable(runnable, redisScheduled.retry());
//        }
        return new LockRunnable(runnable, new RedisLock(
                redis, redisScheduled.key(), Duration.parse(redisScheduled.min()), Duration.parse(redisScheduled.max())));
    }

    @Override
    public void run() {
        delegate.run();
    }
}
