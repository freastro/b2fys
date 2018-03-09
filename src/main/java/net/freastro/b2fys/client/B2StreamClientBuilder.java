package net.freastro.b2fys.client;

import com.backblaze.b2.client.B2ClientConfig;
import com.backblaze.b2.client.B2Sdk;
import com.backblaze.b2.client.webApiHttpClient.B2StorageHttpClientBuilder;
import com.backblaze.b2.client.webApiHttpClient.HttpClientFactory;
import com.backblaze.b2.client.webApiHttpClient.HttpClientFactoryImpl;

import javax.annotation.Nonnull;

/**
 * Builder for {@code B2StreamClient}.
 */
public class B2StreamClientBuilder {

    /**
     * B2 client configuration
     */
    @Nonnull
    private final B2ClientConfig config;

    /**
     * B2 storage client builder
     */
    @Nonnull
    private final B2StorageHttpClientBuilder storageBuilder;

    /**
     * Create a B2 stream client builder.
     */
    public static B2StreamClientBuilder builder(@Nonnull final B2ClientConfig config) {
        return new B2StreamClientBuilder(config);
    }

    /**
     * Construct a {@code B2StreamClient}.
     */
    private B2StreamClientBuilder(@Nonnull final B2ClientConfig config) {
        this.config = config;
        storageBuilder = B2StorageHttpClientBuilder.builder(config);
    }

    /**
     * Build a new {@code B2StreamClient}.
     */
    @Nonnull
    public B2StreamClient build() {
        final HttpClientFactory clientFactory = HttpClientFactoryImpl.build();
        storageBuilder.setHttpClientFactory(clientFactory);
        final String userAgent = config.getUserAgent() + " " + B2Sdk.getName() + "/"
                                 + B2Sdk.getVersion();
        return new B2StreamClient(config, storageBuilder.build(), clientFactory, userAgent);
    }
}
