/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension;

import org.opensearch.dataprepper.plugins.geoip.extension.databasedownload.DBSourceOptions;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Implementation of class for checking whether URL type is S3 or file path
 */
class DatabaseSourceIdentification {

    private DatabaseSourceIdentification() {

    }

    private static final String S3_DOMAIN_PATTERN = "[a-zA-Z0-9-]+\\.s3\\.amazonaws\\.com";
    private static final String MANIFEST_ENDPOINT_PATH = "manifest.json";

    /**
     * Check for database path is valid S3 URI or not
     * @param uriString uriString
     * @return boolean
     */
    public static boolean isS3Uri(final String uriString) {
        try {
            URI uri = new URI(uriString);
            if (uri.getScheme() != null && uri.getScheme().equalsIgnoreCase("s3")) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    /**
     * Check for database path is valid URL or not
     * @param input input
     * @return boolean
     */
    public static boolean isURL(final String input) {
        try {
            final URI uri = new URI(input);
            final URL url = new URL(input);
            return !input.endsWith(MANIFEST_ENDPOINT_PATH) &&
                    !uri.getHost().contains("geoip.maps.opensearch") &&
                    uri.getHost().equals("download.maxmind.com") &&
                    uri.getScheme() != null &&
                    !Pattern.matches(S3_DOMAIN_PATTERN, url.getHost()) &&
                    (uri.getScheme().equals("http") || uri.getScheme().equals("https"));
        } catch (URISyntaxException | MalformedURLException e) {
            return false;
        }
    }

    /**
     * Check for database path is local file path or not
     * @param input input
     * @return boolean
     */
    public static boolean isFilePath(final String input) {
        final File file = new File(input);
        return file.exists() && file.isFile();
    }

    /**
     * Check for database path is CDN endpoint
     * @param input input
     * @return boolean
     */
    public static boolean isCDNEndpoint(final String input) {
        if (input.endsWith(MANIFEST_ENDPOINT_PATH)) {
            try {
                final URI uri = new URI(input);
                return uri.getScheme().equals("http") || uri.getScheme().equals("https");
            } catch (final URISyntaxException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * Get the database path options based on input URL
     * @param databasePaths - List of database paths to get databases data from
     * @return DBSourceOptions
     */
    public static DBSourceOptions getDatabasePathType(final List<String> databasePaths) {
        DBSourceOptions downloadSourceOptions = null;
        for(final String databasePath : databasePaths) {

            if(DatabaseSourceIdentification.isFilePath(databasePath)) {
                return DBSourceOptions.PATH;
            }
            else if (DatabaseSourceIdentification.isCDNEndpoint(databasePath)) {
                return DBSourceOptions.HTTP_MANIFEST;
            }
            else if(DatabaseSourceIdentification.isURL(databasePath))
            {
                return DBSourceOptions.URL;
            }
            else if(DatabaseSourceIdentification.isS3Uri(databasePath))
            {
                return DBSourceOptions.S3;
            }
        }
        return downloadSourceOptions;
    }
}
