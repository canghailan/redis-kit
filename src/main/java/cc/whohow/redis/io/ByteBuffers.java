package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ByteBuffers {
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    /**
     * 空Buffer引用
     */
    public static ByteBuffer empty() {
        return EMPTY;
    }

    /**
     * 是否是空buffer
     */
    public static boolean isEmpty(ByteBuffer byteBuffer) {
        return byteBuffer == null || !byteBuffer.hasRemaining();
    }

    /**
     * 字符串
     */
    public static ByteBuffer from(CharSequence charSequence, Charset charset) {
        return charset.encode(CharBuffer.wrap(charSequence));
    }

    /**
     * 字符串
     */
    public static String toString(ByteBuffer byteBuffer, Charset charset) {
        if (byteBuffer == null) {
            return null;
        }
        if (byteBuffer.hasArray()) {
            return new String(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.remaining(), charset);
        }
        return charset.decode(byteBuffer.duplicate()).toString();
    }

    /**
     * Utf8字符串
     */
    public static ByteBuffer fromUtf8(CharSequence charSequence) {
        return from(charSequence, StandardCharsets.UTF_8);
    }

    /**
     * Utf8字符串
     */
    public static String toUtf8String(ByteBuffer byteBuffer) {
        return toString(byteBuffer, StandardCharsets.UTF_8);
    }

    /**
     * 拷贝
     */
    public static ByteBuffer copy(ByteBuffer bytes) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.remaining());
        byteBuffer.put(bytes.duplicate());
        byteBuffer.flip();
        return byteBuffer;
    }

    /**
     * 连接
     */
    public static ByteBuffer concat(ByteBuffer buffer1, ByteBuffer buffer2) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(buffer1.remaining() + buffer2.remaining());
        byteBuffer.put(buffer1.duplicate());
        byteBuffer.put(buffer2.duplicate());
        byteBuffer.flip();
        return byteBuffer;
    }

    /**
     * 连接
     */
    public static ByteBuffer concat(ByteBuffer... buffers) {
        int capacity = Arrays.stream(buffers).mapToInt(ByteBuffer::remaining).sum();
        ByteBuffer byteBuffer = ByteBuffer.allocate(capacity);
        for (ByteBuffer buffer : buffers) {
            byteBuffer.put(buffer.duplicate());
        }
        byteBuffer.flip();
        return byteBuffer;
    }

    /**
     * 裁剪
     */
    public static ByteBuffer slice(ByteBuffer byteBuffer, int start) {
        ByteBuffer slice = byteBuffer.duplicate();
        slice.position(slice.position() + start);
        return slice;
    }

    /**
     * 是否内容一致
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
     * 是否匹配前缀
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
