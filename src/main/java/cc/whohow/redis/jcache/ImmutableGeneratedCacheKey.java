package cc.whohow.redis.jcache;

import javax.cache.annotation.GeneratedCacheKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 缓存键（不可变）
 */
public abstract class ImmutableGeneratedCacheKey implements GeneratedCacheKey {
    public static ImmutableGeneratedCacheKey empty() {
        return EmptyGeneratedCacheKey.INSTANCE;
    }

    public static ImmutableGeneratedCacheKey ofNull() {
        return SingletonGeneratedCacheKey.NULL;
    }

    public static ImmutableGeneratedCacheKey of(Object key) {
        return key == null ? ofNull() : new SingletonGeneratedCacheKey(key);
    }

    public static ImmutableGeneratedCacheKey of(Object... keys) {
        if (keys.length == 0) {
            return empty();
        }
        if (keys.length == 1) {
            return of(keys[0]);
        }
        return new ArrayGeneratedCacheKey(keys);
    }

    public abstract Object getKey(int index);

    public abstract int size();

    public abstract List<Object> getKeys();

    private static final class EmptyGeneratedCacheKey extends ImmutableGeneratedCacheKey {
        private static final EmptyGeneratedCacheKey INSTANCE = new EmptyGeneratedCacheKey();

        private EmptyGeneratedCacheKey() {
        }

        public Object getKey(int index) {
            throw new IndexOutOfBoundsException();
        }

        public int size() {
            return 0;
        }

        public List<Object> getKeys() {
            return Collections.emptyList();
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof ImmutableGeneratedCacheKey) {
                ImmutableGeneratedCacheKey that = (ImmutableGeneratedCacheKey) o;
                return that.size() == this.size();
            }
            return false;
        }

        @Override
        public String toString() {
            return "";
        }
    }

    private static final class SingletonGeneratedCacheKey extends ImmutableGeneratedCacheKey {
        private static final SingletonGeneratedCacheKey NULL = new SingletonGeneratedCacheKey(null);
        private final Object key;

        private SingletonGeneratedCacheKey(Object key) {
            this.key = key;
        }

        public Object getKey(int index) {
            if (index == 0) {
                return key;
            }
            throw new IndexOutOfBoundsException();
        }

        public int size() {
            return 1;
        }

        public List<Object> getKeys() {
            return Collections.singletonList(key);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(key);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof ImmutableGeneratedCacheKey) {
                ImmutableGeneratedCacheKey that = (ImmutableGeneratedCacheKey) o;
                return that.size() == this.size() && Objects.equals(that.getKey(0), this.getKey(0));
            }
            return false;
        }

        @Override
        public String toString() {
            return Objects.toString(key);
        }
    }

    private static final class ArrayGeneratedCacheKey extends ImmutableGeneratedCacheKey {
        private final Object[] keys;

        private ArrayGeneratedCacheKey(Object[] keys) {
            this.keys = keys;
        }

        @Override
        public Object getKey(int index) {
            return keys[index];
        }

        @Override
        public int size() {
            return keys.length;
        }

        @Override
        public List<Object> getKeys() {
            return Arrays.asList(keys);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(keys);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof ArrayGeneratedCacheKey) {
                ArrayGeneratedCacheKey that = (ArrayGeneratedCacheKey) o;
                return Arrays.equals(that.keys, this.keys);
            }
            if (o instanceof ImmutableGeneratedCacheKey) {
                ImmutableGeneratedCacheKey that = (ImmutableGeneratedCacheKey) o;
                return that.size() == this.size() && Objects.equals(that.getKeys(), this.getKeys());
            }
            return false;
        }

        @Override
        public String toString() {
            return Arrays.toString(keys);
        }
    }
}
