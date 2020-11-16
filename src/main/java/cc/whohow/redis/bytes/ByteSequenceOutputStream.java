package cc.whohow.redis.bytes;

import java.io.ByteArrayOutputStream;

public class ByteSequenceOutputStream extends ByteArrayOutputStream {
    public ByteSequenceOutputStream() {
        super();
    }

    public ByteSequenceOutputStream(int size) {
        super(size);
    }

    public ByteSequence getByteSequence() {
        return new ByteArraySequence(buf, 0, count);
    }
}
