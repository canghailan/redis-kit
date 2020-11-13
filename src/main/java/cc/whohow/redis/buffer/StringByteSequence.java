package cc.whohow.redis.buffer;

import java.nio.CharBuffer;
import java.nio.charset.Charset;

class StringByteSequence extends ByteArraySequence {
    protected final Charset charset;

    public StringByteSequence(String string, Charset charset) {
        super(charset.encode(string));
        this.charset = charset;
    }

    public StringByteSequence(CharSequence charSequence, Charset charset) {
        super(charset.encode(CharBuffer.wrap(charSequence)));
        this.charset = charset;
    }

    public StringByteSequence(CharBuffer charBuffer, Charset charset) {
        super(charset.encode(charBuffer));
        this.charset = charset;
    }

    @Override
    public String toString() {
        return toString(charset);
    }
}
