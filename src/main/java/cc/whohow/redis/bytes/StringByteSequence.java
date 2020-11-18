package cc.whohow.redis.bytes;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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

    static class ASCII extends ByteArraySequence {
        public ASCII(String string) {
            super(StandardCharsets.US_ASCII.encode(string));
        }

        public ASCII(CharSequence charSequence) {
            super(StandardCharsets.US_ASCII.encode(CharBuffer.wrap(charSequence)));
        }

        public ASCII(CharBuffer charBuffer) {
            super(StandardCharsets.US_ASCII.encode(charBuffer));
        }

        @Override
        public String toString() {
            return toString(StandardCharsets.US_ASCII);
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

    static class UTF8 extends ByteArraySequence {
        public UTF8(String string) {
            super(StandardCharsets.UTF_8.encode(string));
        }

        public UTF8(CharSequence charSequence) {
            super(StandardCharsets.UTF_8.encode(CharBuffer.wrap(charSequence)));
        }

        public UTF8(CharBuffer charBuffer) {
            super(StandardCharsets.UTF_8.encode(charBuffer));
        }

        @Override
        public String toString() {
            return toString(StandardCharsets.UTF_8);
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
}
