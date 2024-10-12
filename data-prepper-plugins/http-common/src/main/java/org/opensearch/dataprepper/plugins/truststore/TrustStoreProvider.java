/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.truststore;

import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class TrustStoreProvider {
    private static final Logger LOG = LoggerFactory.getLogger(TrustStoreProvider.class);

    public static TrustManager[] createTrustManager(final Path certificatePath) {
        LOG.info("Using the certificate path {} to create trust manager.", certificatePath.toString());
        try {
            final KeyStore keyStore = createKeyStore(certificatePath);
            final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
            trustManagerFactory.init(keyStore);
            return trustManagerFactory.getTrustManagers();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public static TrustManager[] createTrustManager(final String certificateContent) {
        LOG.info("Using the certificate content to create trust manager.");
        try (InputStream certificateInputStream = new ByteArrayInputStream(certificateContent.getBytes())) {
            final KeyStore keyStore = createKeyStore(certificateInputStream);
            final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
            trustManagerFactory.init(keyStore);
            return trustManagerFactory.getTrustManagers();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public static TrustManager[] createTrustAllManager() {
        LOG.info("Using the trust all manager to create trust manager.");
        return new TrustManager[]{
            new X509TrustAllManager()
        };
    }

    private static KeyStore createKeyStore(final Path certificatePath) throws Exception {
        try (InputStream certificateInputStream = Files.newInputStream(certificatePath)) {
            return createKeyStore(certificateInputStream);
        }
    }

    private static KeyStore createKeyStore(final InputStream trustStoreInputStream, final String password) throws Exception {
        final KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(trustStoreInputStream, password.toCharArray());
        return trustStore;
    }

    private static KeyStore createKeyStore(final InputStream certificateInputStream) throws Exception {
        final CertificateFactory factory = CertificateFactory.getInstance("X.509");
        final Certificate trustedCa = factory.generateCertificate(certificateInputStream);
        final KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        return trustStore;
    }

    public static SSLContext createSSLContext(final Path certificatePath) {
        LOG.info("Using the certificate path to create SSL context.");
        try (InputStream is = Files.newInputStream(certificatePath)) {
            return createSSLContext(is);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public static SSLContext createSSLContext(final Path trustStorePath, final String password) {
        LOG.info("Using the truststore path and password to create SSL context.");
        try (InputStream is = Files.newInputStream(trustStorePath)) {
            return createSSLContext(is, password);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public static SSLContext createSSLContext(final String certificateContent) {
        LOG.info("Using the certificate content to create SSL context.");
        try (InputStream certificateInputStream = new ByteArrayInputStream(certificateContent.getBytes())) {
            return createSSLContext(certificateInputStream);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private static SSLContext createSSLContext(final InputStream certificateInputStream) throws Exception {
        final KeyStore trustStore = createKeyStore(certificateInputStream);
        return SSLContexts.custom()
               .loadTrustMaterial(trustStore, null).build();
    }

    private static SSLContext createSSLContext(final InputStream certificateInputStream, final String password) throws Exception {
        final KeyStore trustStore = createKeyStore(certificateInputStream, password);
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        // Create an SSLContext that uses the truststore
        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        SSLContext.setDefault(sslContext);
        return sslContext;
    }

    public static SSLContext createSSLContextWithTrustAllStrategy() {
        LOG.info("Using the trust all strategy to create SSL context.");
        try {
            return SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy() {
                @Override
                public boolean isTrusted(X509Certificate[] chain, String authType) {
                    return true;
                }
            }).build();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
