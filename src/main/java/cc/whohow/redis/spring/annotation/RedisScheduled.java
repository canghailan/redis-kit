package cc.whohow.redis.spring.annotation;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisScheduled {
    String key();

    String min() default "PT3S";

    String max();
}
