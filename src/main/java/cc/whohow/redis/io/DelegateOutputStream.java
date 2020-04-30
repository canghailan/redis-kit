package cc.whohow.redis.io;

import java.io.IOException;
import java.io.OutputStream;

public class DelegateOutputStream extends OutputStream {
    private final OutputStream delegate;
    private final boolean close;

    public DelegateOutputStream(OutputStream delegate, boolean close) {
        this.delegate = delegate;
        this.close = close;
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        if (close) {
            delegate.close();
        }
    }
}
