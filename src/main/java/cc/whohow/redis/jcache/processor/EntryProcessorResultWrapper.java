package cc.whohow.redis.jcache.processor;

import javax.cache.CacheException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

public class EntryProcessorResultWrapper<T> implements EntryProcessorResult<T> {
    protected final T value;
    protected final CacheException exception;

    public EntryProcessorResultWrapper(T value) {
        this.value = value;
        this.exception = null;
    }

    public EntryProcessorResultWrapper(CacheException exception) {
        this.value = null;
        this.exception = exception;
    }

    @Override
    public T get() throws EntryProcessorException {
        if (exception != null) {
            throw exception;
        }
        return value;
    }
}
