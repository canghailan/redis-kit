package cc.whohow.redis.util;

import java.lang.reflect.Array;

public class ArrayType<T> {
    private final Class<?> componentType;

    public ArrayType(Class<?> componentType) {
        this.componentType = componentType;
    }

    public static <T> ArrayType<T> of(T[] example) {
        return new ArrayType<>(example.getClass().getComponentType());
    }

    public Class<?> getComponentType() {
        return componentType;
    }

    @SuppressWarnings("unchecked")
    public T[] newInstance(int length) {
        return (T[]) Array.newInstance(componentType, length);
    }
}
