package cc.whohow.redis.util.impl;

import java.lang.reflect.Array;
import java.util.function.IntFunction;
import java.util.stream.Stream;

public class NewArray<T> implements IntFunction<T[]> {
    private final Class<?> type;

    public NewArray(Class<?> type) {
        this.type = type;
    }

    public NewArray(T[] example) {
        this(example.getClass().getComponentType());
    }

    public static <T> T[] toArray(Stream<T> stream, T[] example) {
        return stream.toArray(new NewArray<>(example));
    }

    @Override
    @SuppressWarnings("unchecked")
    public T[] apply(int length) {
        return (T[]) Array.newInstance(type, length);
    }
}
