package cc.whohow.redis.buffer;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

class UTF8ByteSequence extends ByteArraySequence {
    public UTF8ByteSequence(String string) {
        super(StandardCharsets.UTF_8.encode(string));
    }

    public UTF8ByteSequence(CharSequence charSequence) {
        super(StandardCharsets.UTF_8.encode(CharBuffer.wrap(charSequence)));
    }

    public UTF8ByteSequence(CharBuffer charBuffer) {
        super(StandardCharsets.UTF_8.encode(charBuffer));
    }

    @Override
    public String toString() {
        return toString(StandardCharsets.UTF_8);
    }
}
