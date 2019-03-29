package cc.whohow.redis.jcache;

import java.util.function.Function;

public class CacheStatisticsLoaderProxy<T, R> implements Function<T, R> {
    private Function<T, R> fn;
    private boolean applied = false;

    public CacheStatisticsLoaderProxy(Function<T, R> fn) {
        this.fn = fn;
    }

    @Override
    public R apply(T t) {
        applied = true;
        return fn.apply(t);
    }

    public boolean isApplied() {
        return applied;
    }
}
