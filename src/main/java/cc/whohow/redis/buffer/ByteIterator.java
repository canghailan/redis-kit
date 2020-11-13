package cc.whohow.redis.buffer;

import java.nio.ByteBuffer;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class ByteIterator implements PrimitiveIterator.OfInt {
    protected final ByteBuffer byteBuffer;
    protected int index;

    public ByteIterator(byte... array) {
        this(array, 0, array.length);
    }

    public ByteIterator(byte[] array, int offset, int length) {
        this(ByteBuffer.wrap(array, offset, length));
    }

    public ByteIterator(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.index = 0;
    }

    public static IntStream stream(byte[] array) {
        return stream(array, 0, array.length);
    }

    public static IntStream stream(byte[] array, int offset, int length) {
        return stream(ByteBuffer.wrap(array, offset, length));
    }

    public static IntStream stream(ByteBuffer byteBuffer) {
        return StreamSupport.intStream(Spliterators.spliterator(
                new ByteIterator(byteBuffer),
                byteBuffer.remaining(),
                Spliterator.ORDERED | Spliterator.SIZED | Spliterator.NONNULL | Spliterator.IMMUTABLE), false);
    }

    public static IntStream stream(OfInt byteIterator) {
        return StreamSupport.intStream(Spliterators.spliteratorUnknownSize(
                byteIterator, Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE), false);
    }


    @Override
    public int nextInt() {
        return byteBuffer.get(index) & 0xff;
    }

    @Override
    public boolean hasNext() {
        return index < byteBuffer.remaining();
    }
}
