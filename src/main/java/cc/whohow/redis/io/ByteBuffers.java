package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

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
