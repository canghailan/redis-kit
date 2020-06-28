package cc.whohow.redis.util.impl;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class RangeIterator<T> implements Iterator<T> {
    protected final Predicate<T> condition;
    protected final BiFunction<T, Integer, T> increment;
    protected T next;
    protected int index;

    public RangeIterator(T init, Predicate<T> condition, BiFunction<T, Integer, T> increment) {
        this.next = init;
        this.index = 0;
        this.condition = condition;
        this.increment = increment;
    }

    @Override
    public boolean hasNext() {
        return condition.test(next);
    }

    @Override
    public T next() {
        T value = this.next;
        this.next = increment.apply(this.next, ++index);
        return value;
    }
}
