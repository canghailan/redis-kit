package cc.whohow.redis.bytes;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;

class ByteBufferSequence implements ByteSequence {
    protected ByteBuffer byteBuffer;

    public ByteBufferSequence(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public int length() {
        return byteBuffer.remaining();
    }

    @Override
    public boolean isEmpty() {
        return byteBuffer.hasRemaining();
    }

    @Override
    public boolean hasArray() {
        return byteBuffer.hasArray();
    }

    @Override
    public byte[] array() {
        return byteBuffer.array();
    }

    @Override
    public int arrayOffset() {
        return byteBuffer.arrayOffset();
    }

    @Override
    public byte get(int index) {
        return byteBuffer.get(index);
    }

    @Override
    public int get(int index, byte[] array, int offset, int length) {
        int n = Integer.min(length, length() - index);
        ByteBuffer duplicate = byteBuffer.duplicate();
        duplicate.position(duplicate.position() + index);
        duplicate.get(array, offset, n);
        return n;
    }

    @Override
    public int get(int index, ByteBuffer buffer) {
        int n = Integer.min(buffer.remaining(), length() - index);
        ByteBuffer duplicate = byteBuffer.duplicate();
        duplicate.position(duplicate.position() + index);
        duplicate.limit(duplicate.position() + n);
        buffer.put(duplicate);
        return n;
    }

    @Override
    public ByteSequence subSequence(int start, int end) {
        ByteBuffer duplicate = byteBuffer.duplicate();
        duplicate.position(duplicate.position() + start);
        duplicate.limit(end - start);
        return new ByteBufferSequence(duplicate);
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return byteBuffer.duplicate();
    }

    @Override
    public CharSequence toCharSequence(Charset charset) {
        return charset.decode(byteBuffer.duplicate());
    }

    @Override
    public IntStream bytes() {
        return ByteStream.of(byteBuffer);
    }

    @Override
    public PrimitiveIterator.OfInt byteIterator() {
        return new ByteIterator(byteBuffer);
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return Collections.singleton(byteBuffer.duplicate()).iterator();
    }

    @Override
    public void forEach(Consumer<? super ByteBuffer> action) {
        action.accept(byteBuffer.duplicate());
    }

    @Override
    public String toString() {
        return toString(StandardCharsets.ISO_8859_1);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ByteSequence) {
            ByteSequence that = (ByteSequence) o;
            return contentEquals(that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ByteSequence.hashCode(this);
    }
}
