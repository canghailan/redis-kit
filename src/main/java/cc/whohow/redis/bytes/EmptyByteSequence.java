package cc.whohow.redis.bytes;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;

class EmptyByteSequence implements ByteSequence {
    private static final EmptyByteSequence INSTANCE = new EmptyByteSequence();

    public static EmptyByteSequence get() {
        return INSTANCE;
    }

    @Override
    public int length() {
        return 0;
    }

    @Override
    public byte get(int index) {
        throw new IndexOutOfBoundsException();
    }

    @Override
    public int get(int index, byte[] array, int offset, int length) {
        throw new IndexOutOfBoundsException();
    }

    @Override
    public ByteSequence subSequence(int start, int end) {
        throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean hasArray() {
        return false;
    }

    @Override
    public IntStream bytes() {
        return IntStream.empty();
    }

    @Override
    public byte[] toByteArray() {
        return new byte[0];
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.allocate(0);
    }

    @Override
    public ByteSequence copy() {
        return this;
    }

    @Override
    public byte[] copyToByteArray() {
        return new byte[0];
    }

    @Override
    public ByteBuffer copyToByteBuffer() {
        return ByteBuffer.allocate(0);
    }

    @Override
    public CharSequence toCharSequence(Charset charset) {
        return "";
    }

    @Override
    public String toString(Charset charset) {
        return "";
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public void forEach(Consumer<? super ByteBuffer> action) {
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ByteSequence) {
            ByteSequence that = (ByteSequence) o;
            return that.isEmpty();
        }
        return false;
    }

    @Override
    public int hashCode() {
//        return ByteSequence.hashCode(this);
        return 1;
    }
}
