package cc.whohow.redis.util.impl;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

public abstract class ConcurrentMapEntrySet<K, V> implements Set<Map.Entry<K, V>> {
    protected final ConcurrentMap<K, V> map;

    public ConcurrentMapEntrySet(ConcurrentMap<K, V> map) {
        this.map = map;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean contains(Object o) {
        if (o instanceof Map.Entry) {
            Map.Entry e = (Map.Entry) o;
            return map.containsKey(e.getKey());
        }
        return false;
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Map.Entry<K, V> e) {
        return map.put(e.getKey(), e.getValue()) != null;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean remove(Object o) {
        if (o instanceof Map.Entry) {
            Map.Entry e = (Map.Entry) o;
            return map.remove(e.getKey()) != null;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Map.Entry<K, V>> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        map.clear();
    }
}
