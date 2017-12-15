package cc.whohow.redis.jcache.annotation;

import java.util.Objects;

public class GeneratedSimpleKey extends GeneratedKey {
    protected GeneratedSimpleKey(Object... keys) {
        super(keys);
    }

    public Object getKey() {
        return keys[0];
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getKey());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof GeneratedSimpleKey) {
            GeneratedSimpleKey that = (GeneratedSimpleKey) o;
            return Objects.equals(this.getKey(), that.getKey());
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toString(getKey());
    }
}
