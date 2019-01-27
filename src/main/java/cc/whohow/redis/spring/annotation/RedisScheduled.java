package cc.whohow.redis.spring.annotation;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisScheduled {
    String key();

    String min() default "PT1M";

    String max();

    int retry() default 0;
}
