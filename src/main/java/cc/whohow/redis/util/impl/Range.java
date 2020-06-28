package cc.whohow.redis.util.impl;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class Range<T> implements Iterable<T> {
    protected final T start;
    protected final Predicate<T> stop;
    protected final BiFunction<T, Integer, T> step;

    public Range(T start, Predicate<T> stop, BiFunction<T, Integer, T> step) {
        this.start = start;
        this.stop = stop;
        this.step = step;
    }

    @Override
    public Iterator<T> iterator() {
        return new RangeIterator<>(start, stop, step);
    }

    public static class MaxBound<T> implements Predicate<T> {
        protected final Comparator<T> comparator;
        protected final boolean inclusive;
        protected final T max;

        public MaxBound(Comparator<T> comparator, boolean inclusive, T max) {
            this.comparator = comparator;
            this.inclusive = inclusive;
            this.max = max;
        }

        @Override
        public boolean test(T value) {
            if (inclusive) {
                return comparator.compare(value, max) > 0;
            } else {
                return comparator.compare(value, max) >= 0;
            }
        }
    }

    public static class MinBound<T> implements Predicate<T> {
        protected final Comparator<T> comparator;
        protected final boolean inclusive;
        protected final T min;

        public MinBound(Comparator<T> comparator, boolean inclusive, T min) {
            this.comparator = comparator;
            this.inclusive = inclusive;
            this.min = min;
        }

        @Override
        public boolean test(T value) {
            if (inclusive) {
                return comparator.compare(value, min) < 0;
            } else {
                return comparator.compare(value, min) <= 0;
            }
        }
    }
}
