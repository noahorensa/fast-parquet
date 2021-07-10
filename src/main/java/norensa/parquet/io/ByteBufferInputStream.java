package norensa.parquet.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {

    private ByteBuffer buf;

    public ByteBufferInputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public int available() throws IOException {
        return buf.remaining();
    }

    @Override
    public void close() throws IOException {
        buf = null;
    }

    @Override
    public int read() throws IOException {
        try {
            return buf.get();
        } catch (BufferUnderflowException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        try {
            buf.get(b);
            return b.length;
        } catch (BufferUnderflowException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            buf.get(b, off, len);
            return len;
        } catch (BufferUnderflowException e) {
            throw new IOException(e);
        }
    }

    int tell() {
        return buf.position();
    }
}
