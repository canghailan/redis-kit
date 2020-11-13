package cc.whohow.redis.buffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;

class ByteArraySequence implements ByteSequence {
    protected byte[] array;
    protected int offset;
    protected int length;

    public ByteArraySequence(byte... array) {
        this(array, 0, array.length);
    }

    public ByteArraySequence(byte[] array, int offset, int length) {
        this.array = array;
        this.offset = offset;
        this.length = length;
    }

    public ByteArraySequence(ByteBuffer byteBuffer) {
        if (byteBuffer.hasArray()) {
            this.array = byteBuffer.array();
            this.offset = byteBuffer.arrayOffset();
            this.length = byteBuffer.remaining();
        } else {
            this.array = new byte[byteBuffer.remaining()];
            this.offset = 0;
            this.length = array.length;
            byteBuffer.get(array, offset, length);
        }
    }

    public static ByteSequence copy(byte... array) {
        return new ByteArraySequence(array).copy();
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public boolean hasArray() {
        return true;
    }

    @Override
    public byte[] array() {
        return array;
    }

    @Override
    public int arrayOffset() {
        return offset;
    }

    @Override
    public byte get(int index) {
        return array[offset + index];
    }

    @Override
    public ByteSequence subSequence(int start, int end) {
        return new ByteArraySequence(array, offset + start, end - start);
    }

    @Override
    public IntStream bytes() {
        return ByteIterator.stream(array, offset, length);
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return Collections.singleton(toByteBuffer()).iterator();
    }

    @Override
    public void forEach(Consumer<? super ByteBuffer> action) {
        action.accept(toByteBuffer());
    }

    @Override
    public String toString() {
        return toString(StandardCharsets.ISO_8859_1);
    }
}
