package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

/**
 * 字符串编码器
 */
public class StringCodec implements Codec<String> {
    /**
     * NULL字符串占位符
     */
    private static final byte NULL_PLACEHOLDER = 127;
    private static final ByteBuffer NULL = ByteBuffer.wrap(new byte[]{NULL_PLACEHOLDER});
    private final Charset charset;

    private volatile long encodeCount = 0;
    private volatile double byteBufferAvgSize;
    private volatile int byteBufferMinSize;
    private volatile int byteBufferMaxSize;
    private volatile long decodeCount = 0;
    private volatile double charBufferAvgSize;
    private volatile int charBufferMinSize;
    private volatile int charBufferMaxSize;

    public StringCodec() {
        this(StandardCharsets.UTF_8);
    }

    public StringCodec(Charset charset) {
        this.charset = charset;
    }

    private static boolean isNull(ByteBuffer buffer) {
        return (buffer == null) || (buffer.remaining() == 1 && buffer.get(0) == NULL_PLACEHOLDER);
    }

    private int getByteBufferSize() {
        return (int) byteBufferAvgSize;
    }

    private int getCharBufferSize() {
        return (int) charBufferAvgSize;
    }

    @Override
    public ByteBuffer encode(String value) {
        if (value == null) {
            return NULL.duplicate();
        }
        CharsetEncoder charsetEncoder = charset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        CharBuffer input = CharBuffer.wrap(value);
        ByteBuffer output = ByteBuffer.allocate(getByteBufferSize());
        while (true) {
            CoderResult cr = input.hasRemaining() ?
                    charsetEncoder.encode(input, output, true) : CoderResult.UNDERFLOW;
            if (cr.isUnderflow()) {
                cr = charsetEncoder.flush(output);
            }

            if (cr.isUnderflow()) {
                break;
            }
            if (cr.isOverflow()) {
                ByteBuffer o = ByteBuffer.allocate(output.capacity() * 2);
                output.flip();
                o.put(output);
                output = o;
            }
        }
        output.flip();
        return output;
    }

    @Override
    public String decode(ByteBuffer input) {
        if (isNull(input)) {
            return null;
        }
        CharsetDecoder charsetDecoder = charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        CharBuffer output = CharBuffer.allocate(getCharBufferSize());
        while (true){
            CoderResult cr = input.hasRemaining() ?
                    charsetDecoder.decode(input, output, true) : CoderResult.UNDERFLOW;
            if (cr.isUnderflow())
                cr = charsetDecoder.flush(output);

            if (cr.isUnderflow())
                break;
            if (cr.isOverflow()) {
                CharBuffer o = CharBuffer.allocate(output.capacity() * 2);
                output.flip();
                o.put(output);
                output = o;
            }
        }
        output.flip();
        return output.toString();
    }

    @Override
    public void encode(String value, OutputStream stream) throws IOException {
        if (value == null) {
            stream.write(NULL_PLACEHOLDER);
            return;
        }
        CharsetEncoder charsetEncoder = charset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        CharBuffer input = CharBuffer.wrap(value);
        ByteBuffer output = ByteBuffer.allocate(getByteBufferSize());
        while (true) {
            CoderResult cr = input.hasRemaining() ?
                    charsetEncoder.encode(input, output, true) : CoderResult.UNDERFLOW;
            if (cr.isUnderflow()) {
                cr = charsetEncoder.flush(output);
            }

            if (cr.isUnderflow()) {
                break;
            }
            if (cr.isOverflow()) {
                output.flip();
                stream.write(output.array(), output.arrayOffset(), output.remaining());
                output.clear();
            }
        }
    }

    @Override
    public String decode(InputStream stream) throws IOException {
        CharsetDecoder charsetDecoder = charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);

        ByteBuffer byteBuffer;
        CharBuffer charBuffer;
        int byte1 = stream.read();
        if (byte1 < 0) {
            return null;
        }
        int byte2 = stream.read();
        if (byte2 < 0) {
            if (byte1 == NULL_PLACEHOLDER) {
                return null;
            } else {
                byteBuffer = ByteBuffer.wrap(new byte[] {(byte) byte1});
                charBuffer = CharBuffer.allocate(1);
            }
        } else {
            byteBuffer = ByteBuffer.allocate(getByteBufferSize());
            byteBuffer.put((byte) byte1);
            byteBuffer.put((byte) byte2);
            charBuffer = CharBuffer.allocate(getCharBufferSize());
        }
        for (;;) {
            CoderResult cr = byteBuffer.hasRemaining() ?
                    charsetDecoder.decode(byteBuffer, charBuffer, true) : CoderResult.UNDERFLOW;
            if (cr.isUnderflow())
                cr = charsetDecoder.flush(charBuffer);

            if (cr.isUnderflow())
                break;
            if (cr.isOverflow()) {
                CharBuffer o = CharBuffer.allocate(charBuffer.capacity() * 2);
                charBuffer.flip();
                o.put(charBuffer);
                charBuffer = o;
            }
        }
        charBuffer.flip();
        return charBuffer.toString();
    }
}
