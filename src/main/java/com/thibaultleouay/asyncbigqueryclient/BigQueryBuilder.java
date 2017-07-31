package com.thibaultleouay.asyncbigqueryclient;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.List;
import java.util.zip.Deflater;

import org.asynchttpclient.DefaultAsyncHttpClientConfig;

import com.google.api.client.auth.oauth2.Credential;

public class BigQueryBuilder {

	private static final URI DEFAULT_URI = URI.create("https://pubsub.googleapis.com/v1/");

	private static final int DEFAULT_REQUEST_TIMEOUT_MS = 30000;

	DefaultAsyncHttpClientConfig.Builder clientConfig = new DefaultAsyncHttpClientConfig.Builder()
			.setCompressionEnforced(true).setUseProxySelector(true).setRequestTimeout(DEFAULT_REQUEST_TIMEOUT_MS)
			.setReadTimeout(DEFAULT_REQUEST_TIMEOUT_MS);

	private Credential credential;
	private URI uri = DEFAULT_URI;
	private int compressionLevel = Deflater.DEFAULT_COMPRESSION;

	public BigQueryBuilder() {
	}

	/**
	 * Creates a new {@code BigQuery}.
	 */
	public BigQuery build() {
		return BigQuery.create(this);
	}

	/**
	 * Set the maximum time in milliseconds the client will can wait when connecting
	 * to a remote host.
	 *
	 * @param connectTimeout
	 *            the connect timeout in milliseconds.
	 * @return this config builder.
	 */
	public BigQueryBuilder connectTimeout(final int connectTimeout) {
		clientConfig.setConnectTimeout(connectTimeout);
		return this;
	}

	/**
	 * Set the Gzip compression level to use, 0-9 or -1 for default.
	 *
	 * @param compressionLevel
	 *            The compression level to use.
	 * @return this config builder.
	 * @see Deflater#setLevel(int)
	 * @see Deflater#DEFAULT_COMPRESSION
	 * @see Deflater#BEST_COMPRESSION
	 * @see Deflater#BEST_SPEED
	 */
	public BigQueryBuilder compressionLevel(final int compressionLevel) {
		checkArgument(compressionLevel > -1 && compressionLevel <= 9, "compressionLevel must be -1 or 0-9.");
		this.compressionLevel = compressionLevel;
		return this;
	}

	/**
	 * Set the connection read timeout in milliseconds.
	 *
	 * @param readTimeout
	 *            the read timeout in milliseconds.
	 * @return this config builder.
	 */
	public BigQueryBuilder readTimeout(final int readTimeout) {
		clientConfig.setReadTimeout(readTimeout);
		return this;
	}

	/**
	 * Set the request timeout in milliseconds.
	 *
	 * @param requestTimeout
	 *            the maximum time in milliseconds.
	 * @return this config builder.
	 */
	public BigQueryBuilder requestTimeout(final int requestTimeout) {
		clientConfig.setRequestTimeout(requestTimeout);
		return this;
	}

	/**
	 * Set the maximum number of connections client will open. Default is unlimited
	 * (-1).
	 *
	 * @param maxConnections
	 *            the maximum number of connections.
	 * @return this config builder.
	 */
	public BigQueryBuilder maxConnections(final int maxConnections) {
		clientConfig.setMaxConnections(maxConnections);
		return this;
	}

	/**
	 * Set the maximum number of milliseconds a pooled connection will be reused. -1
	 * for no limit.
	 *
	 * @param pooledConnectionTTL
	 *            the maximum time in milliseconds.
	 * @return this config builder.
	 */
	public BigQueryBuilder pooledConnectionTTL(final int pooledConnectionTTL) {
		clientConfig.setConnectionTtl(pooledConnectionTTL);
		return this;
	}

	/**
	 * Set the maximum number of milliseconds an idle pooled connection will be be
	 * kept.
	 *
	 * @param pooledConnectionIdleTimeout
	 *            the timeout in milliseconds.
	 * @return this config builder.
	 */
	public BigQueryBuilder pooledConnectionIdleTimeout(final int pooledConnectionIdleTimeout) {
		clientConfig.setPooledConnectionIdleTimeout(pooledConnectionIdleTimeout);
		return this;
	}

	/**
	 * Set Google Cloud API credentials to use. Set to null to use application
	 * default credentials.
	 *
	 * @param credential
	 *            the credentials used to authenticate.
	 */
	public BigQueryBuilder credential(final Credential credential) {
		this.credential = credential;
		return this;
	}

	/**
	 * Set cipher suites to enable for SSL/TLS.
	 *
	 * @param enabledCipherSuites
	 *            The cipher suites to enable.
	 */
	public BigQueryBuilder enabledCipherSuites(final String... enabledCipherSuites) {
		clientConfig.setEnabledCipherSuites(enabledCipherSuites);
		return this;
	}

	/**
	 * Set cipher suites to enable for SSL/TLS.
	 *
	 * @param enabledCipherSuites
	 *            The cipher suites to enable.
	 */
	public BigQueryBuilder enabledCipherSuites(final List<String> enabledCipherSuites) {
		clientConfig.setEnabledCipherSuites(enabledCipherSuites.toArray(new String[enabledCipherSuites.size()]));
		return this;
	}

	/**
	 * The Google Cloud Pub/Sub API URI. By default, this is the Google Cloud
	 * Pub/Sub provider, however you may run a local Developer Server.
	 *
	 * @param uri
	 *            the service to connect to.
	 */
	public BigQueryBuilder uri(final URI uri) {
		checkNotNull(uri, "uri");
		checkArgument(uri.getRawQuery() == null, "illegal service uri: %s", uri);
		checkArgument(uri.getRawFragment() == null, "illegal service uri: %s", uri);
		this.uri = uri;
		return this;
	}
}
