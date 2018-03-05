package net.freastro.b2fys.client;

import com.backblaze.b2.client.contentSources.B2Headers;
import com.backblaze.b2.client.contentSources.B2HeadersImpl;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An input stream from B2.
 */
public class B2Stream extends InputStream {

    private static final String SOCKET_CLOSED_MSG = "Socket is closed";

    /**
     * Cached response headers
     */
    @Nullable
    private B2Headers headers;

    /**
     * Cached response entity input stream
     */
    @Nullable
    private InputStream in;

    /**
     * HTTP response
     */
    @Nonnull
    private final CloseableHttpResponse response;

    /**
     * Constructs a {@code B2Stream}.
     */
    public B2Stream(@Nonnull final CloseableHttpResponse response) {
        this.response = response;
    }

    /**
     * Gets the input stream for the response entity.
     */
    @Nonnull
    public InputStream getInputStream() throws IOException {
        if (in == null) {
            in = response.getEntity().getContent();
        }
        return in;
    }

    /**
     * Gets the HTTP headers of the response.
     */
    @Nonnull
    public B2Headers getResponseHeaders() {
        if (headers == null) {
            final B2HeadersImpl.Builder builder = B2HeadersImpl.builder();
            for (Header header : response.getAllHeaders()) {
                builder.set(header.getName(), header.getValue());
            }
            this.headers = builder.build();
        }
        return headers;
    }

    @Override
    public int read() throws IOException {
        try {
            return getInputStream().read();
        } catch (final SocketException e) {
            if (SOCKET_CLOSED_MSG.equals(e.getMessage())) {
                return -1;
            }
            throw e;
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        try {
            return getInputStream().read(b);
        } catch (final SocketException e) {
            if (SOCKET_CLOSED_MSG.equals(e.getMessage())) {
                return -1;
            }
            throw e;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            return getInputStream().read(b, off, len);
        } catch (final SocketException e) {
            if (SOCKET_CLOSED_MSG.equals(e.getMessage())) {
                return -1;
            }
            throw e;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        return getInputStream().skip(n);
    }

    @Override
    public int available() throws IOException {
        return getInputStream().available();
    }

    @Override
    public void close() throws IOException {
        response.close();
    }

    @Override
    public void mark(int readlimit) {
        try {
            getInputStream().mark(readlimit);
        } catch (final IOException e) {
            // ignored
        }
    }

    @Override
    public void reset() throws IOException {
        getInputStream().reset();
    }

    @Override
    public boolean markSupported() {
        try {
            return getInputStream().markSupported();
        } catch (final IOException e) {
            return false;
        }
    }
}
