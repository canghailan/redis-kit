package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ByteBuffers {
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    public static ByteBuffer empty() {
        return EMPTY;
    }

    public static boolean isEmpty(ByteBuffer byteBuffer) {
        return byteBuffer == null || !byteBuffer.hasRemaining();
    }

    public static ByteBuffer fromUtf8(CharSequence charSequence) {
        return StandardCharsets.UTF_8.encode(CharBuffer.wrap(charSequence));
    }

    public static String toUtf8String(ByteBuffer byteBuffer) {
        return byteBuffer == null ? null : StandardCharsets.UTF_8.decode(byteBuffer.duplicate()).toString();
    }

    public static ByteBuffer copy(ByteBuffer bytes) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.remaining());
        byteBuffer.put(bytes.duplicate());
        byteBuffer.flip();
        return byteBuffer;
    }

    public static ByteBuffer concat(ByteBuffer buffer1, ByteBuffer buffer2) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(buffer1.remaining() + buffer2.remaining());
        byteBuffer.put(buffer1.duplicate());
        byteBuffer.put(buffer2.duplicate());
        byteBuffer.flip();
        return byteBuffer;
    }

    public static ByteBuffer concat(ByteBuffer... buffers) {
        int capacity = Arrays.stream(buffers).mapToInt(ByteBuffer::remaining).sum();
        ByteBuffer byteBuffer = ByteBuffer.allocate(capacity);
        for (ByteBuffer buffer : buffers) {
            byteBuffer.put(buffer.duplicate());
        }
        byteBuffer.flip();
        return byteBuffer;
    }

    public static ByteBuffer slice(ByteBuffer byteBuffer, int start) {
        ByteBuffer slice = byteBuffer.duplicate();
        slice.position(slice.position() + start);
        return slice;
    }

    /**
     * 内容一致
     */
    public static boolean contentEquals(ByteBuffer a, ByteBuffer b) {
        if (a.remaining() != b.remaining()) {
            return false;
        }
        for (int i = a.position(), j = b.position(); i < a.limit(); i++, j++) {
            if (a.get(i) != b.get(j)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 匹配前缀
     */
    public static boolean startsWith(ByteBuffer byteBuffer, ByteBuffer prefix) {
        if (byteBuffer.remaining() < prefix.remaining()) {
            return false;
        }
        for (int i = byteBuffer.position(), j = prefix.position(); j < prefix.limit(); i++, j++) {
            if (byteBuffer.get(i) != prefix.get(j)) {
                return false;
            }
        }
        return true;
    }
}
