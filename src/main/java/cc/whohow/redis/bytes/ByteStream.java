package cc.whohow.redis.bytes;

import java.nio.ByteBuffer;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class ByteStream {
    public static IntStream of(byte... array) {
        return of(array, 0, array.length);
    }

    public static IntStream of(byte[] array, int offset, int length) {
        return stream(new ByteIterator(array, offset, length), length);
    }

    public static IntStream of(ByteBuffer byteBuffer) {
        return stream(new ByteIterator(byteBuffer), byteBuffer.remaining());
    }

    public static IntStream stream(PrimitiveIterator.OfInt byteIterator) {
        return StreamSupport.intStream(Spliterators.spliteratorUnknownSize(
                byteIterator, Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE), false);
    }

    public static IntStream stream(PrimitiveIterator.OfInt byteIterator, int size) {
        return StreamSupport.intStream(Spliterators.spliterator(
                byteIterator,
                size,
                Spliterator.ORDERED | Spliterator.SIZED | Spliterator.NONNULL | Spliterator.IMMUTABLE), false);
    }
}
