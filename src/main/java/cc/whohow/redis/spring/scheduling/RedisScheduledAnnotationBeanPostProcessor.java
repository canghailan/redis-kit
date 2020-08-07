package cc.whohow.redis.spring.scheduling;

import cc.whohow.redis.RedisLockFactory;
import cc.whohow.redis.spring.annotation.RedisScheduled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.locks.Lock;

public class RedisScheduledAnnotationBeanPostProcessor extends ScheduledAnnotationBeanPostProcessor {
    private static final Logger log = LogManager.getLogger();
    protected final RedisLockFactory redisLockFactory;

    public RedisScheduledAnnotationBeanPostProcessor(RedisLockFactory redisLockFactory) {
        this.redisLockFactory = redisLockFactory;
    }

    @Override
    protected Runnable createRunnable(Object target, Method method) {
        Runnable runnable = super.createRunnable(target, method);
        RedisScheduled redisScheduled = AnnotationUtils.findAnnotation(method, RedisScheduled.class);
        if (redisScheduled == null) {
            return runnable;
        } else {
            log.debug("RedisScheduled: {}", runnable);
            return new TryLockRunnable(runnable, createLock(redisScheduled));
        }
    }

    protected Lock createLock(RedisScheduled redisScheduled) {
        return redisLockFactory.newLock(redisScheduled.key(), Duration.parse(redisScheduled.min()), Duration.parse(redisScheduled.max()));
    }
}
