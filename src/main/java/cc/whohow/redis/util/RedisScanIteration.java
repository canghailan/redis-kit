package cc.whohow.redis.util;

import java.util.ArrayList;
import java.util.List;

public class RedisScanIteration<T> {
    protected String cursor;
    protected List<T> array;

    public RedisScanIteration() {
        this(new ArrayList<>());
    }

    public RedisScanIteration(List<T> array) {
        this.array = array;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public List<T> getArray() {
        return array;
    }

    public boolean isTerminate() {
        return "0".equals(cursor);
    }
}
