package cc.whohow.redis.util;

import cc.whohow.redis.io.Codec;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class CodecIterator<T> implements Iterator<T> {
    private Iterator<ByteBuffer> iterator;
    private Codec<T> codec;

    public CodecIterator(Iterator<ByteBuffer> iterator, Codec<T> codec) {
        this.iterator = iterator;
        this.codec = codec;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return codec.decode(iterator.next());
    }

    @Override
    public void remove() {
        iterator.remove();
    }
}
