package cc.whohow.redis.spring.annotation;

import cc.whohow.redis.RedisFactory;
import cc.whohow.redis.util.RedisLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.annotation.Order;

import java.lang.reflect.Method;
import java.time.Duration;

@Order
@Aspect
public class RedisScheduledAspect {
    private static final Logger log = LogManager.getLogger();
    private final RedisFactory redisFactory;

    public RedisScheduledAspect(RedisFactory redisFactory) {
        this.redisFactory = redisFactory;
    }

    @Around("@annotation(cc.whohow.redis.spring.annotation.RedisScheduled)")
    public Object lock(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        RedisScheduled redisScheduled = method.getAnnotation(RedisScheduled.class);
        RedisLock lock = redisFactory.newLock(redisScheduled.key(),
                Duration.parse(redisScheduled.min()), Duration.parse(redisScheduled.max()));
        log.trace("tryLock: {} {} {}", redisScheduled.key(), redisScheduled.min(), redisScheduled.max());
        if (lock.tryLock()) {
            try {
                log.info("locked: {}", redisScheduled.key());
                log.trace("proceed: {}", signature);
                return joinPoint.proceed();
            } finally {
                lock.unlock();
                log.info("unlock: {}", redisScheduled.key());
            }
        }
        return null;
    }
}
