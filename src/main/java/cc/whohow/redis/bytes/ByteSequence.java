package cc.whohow.redis.bytes;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public interface ByteSequence extends Iterable<ByteBuffer>, Comparable<ByteSequence> {
    static ByteSequence empty() {
        return EmptyByteSequence.get();
    }

    static ByteSequence of(byte... byteArray) {
        return new ByteArraySequence(byteArray);
    }

    static ByteSequence of(byte[] byteArray, int offset, int length) {
        return new ByteArraySequence(byteArray, offset, length);
    }

    static ByteSequence of(ByteBuffer byteBuffer) {
        return new ByteBufferSequence(byteBuffer);
    }

    static ByteSequence of(String string, Charset charset) {
        return new StringByteSequence(string, charset);
    }

    static ByteSequence of(CharSequence charSequence, Charset charset) {
        return new StringByteSequence(charSequence, charset);
    }

    static ByteSequence of(CharBuffer charBuffer, Charset charset) {
        return new StringByteSequence(charBuffer, charset);
    }

    static ByteSequence ascii(String string) {
        return new StringByteSequence.ASCII(string);
    }

    static ByteSequence ascii(CharSequence charSequence) {
        return new StringByteSequence.ASCII(charSequence);
    }

    static ByteSequence ascii(CharBuffer charBuffer) {
        return new StringByteSequence.ASCII(charBuffer);
    }

    static ByteSequence utf8(String string) {
        return new StringByteSequence.UTF8(string);
    }

    static ByteSequence utf8(CharSequence charSequence) {
        return new StringByteSequence.UTF8(charSequence);
    }

    static ByteSequence utf8(CharBuffer charBuffer) {
        return new StringByteSequence.UTF8(charBuffer);
    }

    static ByteSequence copy(byte... byteArray) {
        return of(byteArray).copy();
    }

    static ByteSequence copy(ByteBuffer byteBuffer) {
        return of(byteBuffer).copy();
    }

    /**
     * @see Arrays#hashCode(byte[])
     * @see ByteSequence#contentEquals(cc.whohow.redis.bytes.ByteSequence)
     */
    static int hashCode(ByteSequence bytes) {
        if (bytes == null) {
            return 0;
        }
        PrimitiveIterator.OfInt iterator = bytes.byteIterator();
        int result = 1;
        while (iterator.hasNext()) {
            result = 31 * result + iterator.nextInt();
        }
        return result;
    }

    /**
     * 长度
     */
    int length();

    /**
     * 是否为空
     */
    default boolean isEmpty() {
        return length() == 0;
    }

    /**
     * 是否有底层数组
     */
    default boolean hasArray() {
        return false;
    }

    /**
     * 底层数组
     */
    default byte[] array() {
        throw new UnsupportedOperationException();
    }

    /**
     * 底层数组偏移
     */
    default int arrayOffset() {
        throw new UnsupportedOperationException();
    }

    /**
     * 读取指定字节
     */
    byte get(int index);

    /**
     * 批量读取指定字节
     */
    default int get(int index, byte[] array) {
        return get(index, array, 0, array.length);
    }

    /**
     * 批量读取指定字节
     */
    default int get(int index, byte[] array, int offset, int length) {
        int n = Integer.min(length, length() - index);
        if (hasArray()) {
            System.arraycopy(array(), arrayOffset() + index, array, offset, n);
        } else {
            for (int i = 0; i < n; i++) {
                array[offset + i] = get(index + i);
            }
        }
        return n;
    }

    /**
     * 批量读取指定字节
     */
    default int get(int index, ByteBuffer buffer) {
        int n = Integer.min(buffer.remaining(), length() - index);
        if (hasArray()) {
            buffer.put(array(), arrayOffset() + index, n);
        } else {
            for (int i = 0; i < n; i++) {
                buffer.put(get(index + i));
            }
        }
        return n;
    }

    /**
     * 子字节序列
     */
    default ByteSequence subSequence(int start, int end) {
        return new SubByteSequence(this, start, end - start);
    }

    /**
     * 字节流
     */
    default IntStream bytes() {
        return StreamSupport.stream(spliterator(), false)
                .flatMapToInt(ByteStream::of);
    }

    /**
     * 字节迭代器
     */
    default PrimitiveIterator.OfInt byteIterator() {
        return bytes().iterator();
    }

    /**
     * 转为字节数组（可能直接返回底层数组）
     */
    default byte[] toByteArray() {
        if (hasArray()) {
            byte[] array = array();
            int offset = arrayOffset();
            int length = length();
            if (offset == 0 && length == array.length) {
                return array;
            }
        }
        return copyToByteArray();
    }

    /**
     * 转为字节缓冲区（可能直接返回底层字节缓冲区）
     */
    default ByteBuffer toByteBuffer() {
        if (hasArray()) {
            return ByteBuffer.wrap(array(), arrayOffset(), length());
        }
        return copyToByteBuffer();
    }

    /**
     * 转为字符序列
     */
    default CharSequence toCharSequence(Charset charset) {
        if (hasArray()) {
            return new String(array(), arrayOffset(), length(), charset);
        } else {
            StringBuilder charSequence = new StringBuilder();
            for (ByteBuffer byteBuffer : this) {
                charSequence.append(charset.decode(byteBuffer.duplicate()));
            }
            return charSequence;
        }
    }

    /**
     * 转为字符串
     */
    default String toString(Charset charset) {
        return toCharSequence(charset).toString();
    }

    /**
     * 复制（可能变为不同类型字节序列）
     */
    default ByteSequence copy() {
        return new ByteArraySequence(copyToByteArray());
    }

    /**
     * 复制为字节数组
     */
    default byte[] copyToByteArray() {
        byte[] array = new byte[length()];
        get(0, array);
        return array;
    }

    /**
     * 复制为字节缓冲区
     */
    default ByteBuffer copyToByteBuffer() {
        return ByteBuffer.wrap(copyToByteArray());
    }

    @Override
    default int compareTo(ByteSequence that) {
        PrimitiveIterator.OfInt thisBytes = byteIterator();
        PrimitiveIterator.OfInt thatBytes = that.byteIterator();
        while (thisBytes.hasNext()) {
            if (thatBytes.hasNext()) {
                int c = Integer.compare(thisBytes.nextInt(), thatBytes.nextInt());
                if (c == 0) {
                    continue;
                }
                return c;
            } else {
                return -1;
            }
        }
        if (thatBytes.hasNext()) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 检查字节序列内容是否相同
     */
    default boolean contentEquals(ByteSequence that) {
        if (length() != that.length()) {
            return false;
        }
        PrimitiveIterator.OfInt thisBytes = byteIterator();
        PrimitiveIterator.OfInt thatBytes = that.byteIterator();
        while (thisBytes.hasNext()) {
            if (thatBytes.hasNext()) {
                if (thisBytes.nextInt() != thatBytes.nextInt()) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * 检查字节起始序列
     */
    default boolean startsWiths(ByteSequence that) {
        if (length() > that.length()) {
            return false;
        }
        PrimitiveIterator.OfInt thisBytes = byteIterator();
        PrimitiveIterator.OfInt thatBytes = that.byteIterator();
        while (thisBytes.hasNext()) {
            if (thatBytes.hasNext()) {
                if (thisBytes.nextInt() != thatBytes.nextInt()) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }
}
