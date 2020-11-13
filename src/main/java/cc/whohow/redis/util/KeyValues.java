package cc.whohow.redis.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class KeyValues<K, V> extends LinkedHashMap<K, V> {
    public KeyValues(Collection<? extends K> keys, Collection<? extends V> values) {
        super(keys.size());
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException();
        }
        Iterator<? extends K> keyIterator = keys.iterator();
        Iterator<? extends V> valueIterator = values.iterator();
        while (keyIterator.hasNext()) {
            if (valueIterator.hasNext()) {
                put(keyIterator.next(), valueIterator.next());
            }
        }
    }
}
