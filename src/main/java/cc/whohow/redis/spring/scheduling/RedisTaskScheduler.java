package cc.whohow.redis.spring.scheduling;

import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;

public class RedisTaskScheduler implements TaskScheduler {
    protected final TaskScheduler taskScheduler;
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;

    public RedisTaskScheduler(TaskScheduler taskScheduler, RedisCommands<ByteBuffer, ByteBuffer> redis) {
        this.taskScheduler = taskScheduler;
        this.redis = redis;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
        return taskScheduler.schedule(proxy(task), trigger);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
        return taskScheduler.schedule(proxy(task), startTime);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
        return taskScheduler.scheduleAtFixedRate(proxy(task), startTime, period);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
        return taskScheduler.scheduleAtFixedRate(proxy(task), period);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
        return taskScheduler.scheduleWithFixedDelay(proxy(task), startTime, delay);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
        return taskScheduler.scheduleWithFixedDelay(proxy(task), delay);
    }

    private Runnable proxy(Runnable runnable) {
        if (runnable instanceof RedisScheduledTask) {
            return runnable;
        } else {
            return new RedisScheduledTask(runnable, redis);
        }
    }
}
