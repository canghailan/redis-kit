package cc.whohow.redis.util.impl;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class RangeIterator<T> implements Iterator<T> {
    protected final Predicate<T> stop;
    protected final BiFunction<T, Integer, T> step;
    protected T next;
    protected int index;

    public RangeIterator(T start, Predicate<T> stop, BiFunction<T, Integer, T> step) {
        this.next = start;
        this.index = 0;
        this.stop = stop;
        this.step = step;
    }

    @Override
    public boolean hasNext() {
        return !stop.test(next);
    }

    @Override
    public T next() {
        T value = next;
        next = step.apply(next, ++index);
        return value;
    }
}
