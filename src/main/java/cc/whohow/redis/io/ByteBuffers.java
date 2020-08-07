package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ByteBuffers {
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
    private static final ByteBuffer[] EMPTY_ARRAY = new ByteBuffer[0];

    /**
     * 空Buffer引用
     */
    public static ByteBuffer empty() {
        return EMPTY;
    }

    public static ByteBuffer[] emptyArray() {
        return EMPTY_ARRAY;
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
            return new String(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), byteBuffer.remaining(), charset);
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

    public static String toString(ByteBuffer byteBuffer) {
        return toString(byteBuffer, StandardCharsets.ISO_8859_1);
    }

    /**
     * 拷贝
     */
    public static ByteBuffer copy(ByteBuffer byteBuffer) {
        ByteBuffer copy = ByteBuffer.allocate(byteBuffer.remaining());
        copy.put(byteBuffer.duplicate());
        copy.flip();
        return copy;
    }

    /**
     * 拷贝
     */
    public static ByteBuffer resize(ByteBuffer byteBuffer, int newCapacity) {
        ByteBuffer src = byteBuffer.duplicate();
        ByteBuffer dst = ByteBuffer.allocate(newCapacity);
        int position = src.position();
        src.flip();
        dst.put(src);
        dst.position(position);
        dst.limit(dst.capacity());
        return dst;
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
