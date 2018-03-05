package net.freastro.b2fys.client;

import com.backblaze.b2.client.B2ClientConfig;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentSources.B2Headers;
import com.backblaze.b2.client.exceptions.B2ConnectFailedException;
import com.backblaze.b2.client.exceptions.B2ConnectionBrokenException;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.exceptions.B2NetworkException;
import com.backblaze.b2.client.exceptions.B2NetworkTimeoutException;
import com.backblaze.b2.client.structures.B2DownloadByIdRequest;
import com.backblaze.b2.client.structures.B2DownloadByNameRequest;
import com.backblaze.b2.client.structures.B2ErrorStructure;
import com.backblaze.b2.client.webApiHttpClient.B2WebApiHttpClientImpl;
import com.backblaze.b2.client.webApiHttpClient.HttpClientFactory;
import com.backblaze.b2.json.B2Json;
import com.backblaze.b2.util.B2ByteRange;

import org.apache.http.NoHttpResponseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Provides streams for downloading content from Backblaze B2.
 */
public class B2StreamClient {

    /**
     * HTTP client factory
     */
    @Nonnull
    private final HttpClientFactory clientFactory;

    /**
     * B2 client configuration
     */
    @Nonnull
    private final B2ClientConfig config;

    /**
     * B2 storage client
     */
    @Nonnull
    private final B2StorageClient storageClient;

    /**
     * HTTP user agent
     */
    @Nonnull
    private final String userAgent;

    /**
     * Constructs a {@code B2StreamClient}.
     */
    public B2StreamClient(@Nonnull final B2ClientConfig config,
                          @Nonnull final B2StorageClient storageClient,
                          @Nonnull final HttpClientFactory clientFactory,
                          @Nonnull final String userAgent) {
        this.config = config;
        this.storageClient = storageClient;
        this.clientFactory = clientFactory;
        this.userAgent = userAgent;
    }

    @Nonnull
    public B2StorageClient getStorageClient() {
        return storageClient;
    }

    /**
     * Downloads the specified file by id.
     */
    @Nonnull
    public B2Stream streamById(B2DownloadByIdRequest request) throws B2Exception {
        return getStream(storageClient.getDownloadByIdUrl(request), request.getRange());
    }

    /**
     * Downloads the specified file by id.
     */
    @Nonnull
    public B2Stream streamById(String fileId) throws B2Exception {
        return streamById(B2DownloadByIdRequest.builder(fileId).build());
    }

    /**
     * Downloads the specified file by name.
     */
    @Nonnull
    public B2Stream streamByName(B2DownloadByNameRequest request) throws B2Exception {
        return getStream(storageClient.getDownloadByNameUrl(request), request.getRange());
    }

    /**
     * Downloads the specified file by name.
     */
    @Nonnull
    public B2Stream streamByName(String bucketName, String fileName) throws B2Exception {
        return streamByName(B2DownloadByNameRequest.builder(bucketName, fileName).build());
    }

    /**
     * Streams the response for the specified URL.
     *
     * @see B2WebApiHttpClientImpl#getContent(java.lang.String, com.backblaze.b2.client.contentSources.B2Headers, com.backblaze.b2.client.contentHandlers.B2ContentSink)
     */
    @Nonnull
    private B2Stream getStream(@Nonnull final String url, @Nullable final B2ByteRange range)
            throws B2Exception {
        // Build request
        final HttpGet get = new HttpGet(url);
        get.setHeader(B2Headers.AUTHORIZATION,
                      storageClient.getAccountAuthorization().getAuthorizationToken());
        get.setHeader(B2Headers.USER_AGENT, userAgent);
        if (range != null) {
            get.setHeader(B2Headers.RANGE, range.toString());
        }

        // Process response
        try {
            final CloseableHttpResponse response = clientFactory.create().execute(get);
            final int statusCode = response.getStatusLine().getStatusCode();
            if (200 <= statusCode && statusCode < 300) {
                return new B2Stream(response);
            } else {
                final String responseText = EntityUtils.toString(response.getEntity(),
                                                                 "UTF-8");
                try {
                    final B2ErrorStructure err =
                            B2Json.get().fromJson(responseText, B2ErrorStructure.class);
                    throw B2Exception.create(err.code, err.status, null, err.message);
                } catch (final Throwable t) {
                    throw new B2Exception("unknown", statusCode, null, responseText);
                }
            }
        } catch (final B2Exception e) {
            throw e;
        } catch (final ConnectException e) {
            throw new B2ConnectFailedException("connect_failed", null,
                                               "failed to connect for " + url, e);
        } catch (final UnknownHostException e) {
            throw new B2ConnectFailedException("unknown_host", null, "unknown host for " + url, e);
        } catch (final ConnectionPoolTimeoutException e) {
            throw new B2ConnectFailedException("connect_timed_out", null,
                                               "connect timed out for " + url, e);
        } catch (final ConnectTimeoutException e) {
            throw new B2ConnectFailedException("connection_pool_timed_out", null,
                                               "connection pool timed out for " + url, e);
        } catch (final SocketTimeoutException e) {
            throw new B2NetworkTimeoutException("socket_timeout", null,
                                                "socket timed out talking to " + url, e);
        } catch (final SocketException e) {
            throw new B2NetworkException("socket_exception", null,
                                         "socket exception talking to " + url, e);
        } catch (final NoHttpResponseException e) {
            throw new B2ConnectionBrokenException("no_http_response", null,
                                                  "didn't get an http response from " + url, e);
        } catch (final IOException e) {
            throw new B2NetworkException("io_exception", null, e + " talking to " + url, e);
        } catch (final Exception e) {
            throw new B2Exception("unexpected", 500, null, "unexpected: " + e, e);
        }
    }
}
