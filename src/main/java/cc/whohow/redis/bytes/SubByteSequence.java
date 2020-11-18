package cc.whohow.redis.bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class SubByteSequence implements ByteSequence {
    protected final ByteSequence byteSequence;
    protected final int offset;
    protected final int length;

    public SubByteSequence(ByteSequence byteSequence, int offset, int length) {
        this.byteSequence = byteSequence;
        this.length = length;
        this.offset = offset;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public boolean hasArray() {
        return byteSequence.hasArray();
    }

    @Override
    public byte[] array() {
        return byteSequence.array();
    }

    @Override
    public int arrayOffset() {
        return byteSequence.arrayOffset() + offset;
    }

    @Override
    public byte get(int index) {
        return byteSequence.get(offset + index);
    }

    @Override
    public ByteSequence subSequence(int start, int end) {
        return new SubByteSequence(byteSequence, offset + start, end - start);
    }

    @Override
    public IntStream bytes() {
        return byteSequence.bytes().skip(offset).limit(length);
    }

    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer byteBuffer = byteSequence.toByteBuffer().duplicate();
        byteBuffer.position(byteBuffer.position() + offset);
        byteBuffer.limit(byteBuffer.position() + length);
        return byteBuffer;
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        List<ByteBuffer> list = new ArrayList<>();
        int index = 0;
        for (ByteBuffer byteBuffer : byteSequence) {
            int n = byteBuffer.remaining();
            if (index >= offset) {
                if (index + n >= length) {
                    byteBuffer.limit(byteBuffer.position() + length - index);
                    list.add(byteBuffer);
                    break;
                } else {
                    list.add(byteBuffer);
                }
            }
            index += n;
        }
        return list.iterator();
    }

    @Override
    public void forEach(Consumer<? super ByteBuffer> action) {
        int index = 0;
        for (ByteBuffer byteBuffer : byteSequence) {
            int n = byteBuffer.remaining();
            if (index >= offset) {
                if (index + n >= length) {
                    byteBuffer.limit(byteBuffer.position() + length - index);
                    action.accept(byteBuffer);
                    break;
                } else {
                    action.accept(byteBuffer);
                }
            }
            index += n;
        }
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