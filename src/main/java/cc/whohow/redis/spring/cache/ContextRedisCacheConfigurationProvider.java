package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.annotation.RedisCacheable;
import cc.whohow.redis.jcache.configuration.AnnotationRedisCacheConfiguration;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ContextRedisCacheConfigurationProvider implements Function<String, RedisCacheConfiguration<?, ?>> {
    private static final Logger log = LogManager.getLogger();
    private final ApplicationContext applicationContext;
    private final Map<String, RedisCacheConfiguration<?, ?>> cacheConfigurations = new ConcurrentHashMap<>();

    public ContextRedisCacheConfigurationProvider(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public RedisCacheConfiguration<?, ?> apply(String name) {
        RedisCacheConfiguration<?, ?> cacheConfiguration = cacheConfigurations.get(name);
        if (cacheConfiguration == null) {
            synchronized (this) {
                cacheConfiguration = cacheConfigurations.get(name);
                if (cacheConfiguration == null) {
                    scan();
                    cacheConfiguration = cacheConfigurations.get(name);
                }
            }
        }
        return cacheConfiguration;
    }

    protected void scan() {
        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Method[] methods = AopUtils.getTargetClass(applicationContext.getBean(beanName)).getDeclaredMethods();
            for (Method method : methods) {
                if (method.isAnnotationPresent(RedisCacheable.class)) {
                    RedisCacheConfiguration<?, ?> cacheConfiguration = new AnnotationRedisCacheConfiguration(method);
                    log.debug("scan cache: {}", cacheConfiguration.getName());
                    cacheConfigurations.putIfAbsent(cacheConfiguration.getName(), cacheConfiguration);
                }
            }
        }
    }
}
