package cc.whohow.redis.bytes;

import cc.whohow.redis.util.IteratorIterator;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class ConcatByteSequence implements ByteSequence {
    protected final ByteSequence head;
    protected final ByteSequence tail;

    public ConcatByteSequence(ByteSequence head, ByteSequence tail) {
        this.head = head;
        this.tail = tail;
    }

    @Override
    public int length() {
        return head.length() + tail.length();
    }

    @Override
    public byte get(int index) {
        int s = head.length();
        if (index < s) {
            return head.get(index);
        } else {
            return tail.get(index - s);
        }
    }

    @Override
    public int get(int index, byte[] array, int offset, int length) {
        int n = Integer.min(length, length() - index);
        int s = head.length();
        if (index >= s) {
            tail.get(index - s, array, offset, n);
        } else if (index + n < s) {
            head.get(index, array, offset, n);
        } else {
            int len = s - index;
            head.get(index, array, offset, len);
            tail.get(s, array, offset + len, n - len);
        }
        return n;
    }

    @Override
    public ByteSequence subSequence(int start, int end) {
        int s = head.length();
        if (end < s) {
            return head.subSequence(start, end);
        }
        if (start >= s) {
            return tail.subSequence(start - s, end - s);
        }
        return new SubByteSequence(this, start, end);
    }

    @Override
    public IntStream bytes() {
        return IntStream.concat(head.bytes(), tail.bytes());
    }

    @Override
    public CharSequence toCharSequence(Charset charset) {
        CharSequence h = head.toCharSequence(charset);
        CharSequence t = tail.toCharSequence(charset);
        return new StringBuilder(h.length() + t.length()).append(h).append(t);
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return new IteratorIterator<>(Arrays.asList(head.iterator(), tail.iterator()));
    }

    @Override
    public void forEach(Consumer<? super ByteBuffer> action) {
        head.forEach(action);
        tail.forEach(action);
    }

    @Override
    public String toString() {
        return head.toString() + tail.toString();
    }
}
