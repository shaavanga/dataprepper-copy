/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension;

import org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.geoip.GeoIPField;
import org.opensearch.dataprepper.plugins.geoip.extension.api.GeoIPDatabaseReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class AutoCountingDatabaseReader implements GeoIPDatabaseReader {
    private static final Logger LOG = LoggerFactory.getLogger(AutoCountingDatabaseReader.class);
    private final GeoIPDatabaseReader delegateDatabaseReader;
    private final AtomicInteger closeCount;

    public AutoCountingDatabaseReader(final GeoIPDatabaseReader geoIPDatabaseReader) {
        this.delegateDatabaseReader = geoIPDatabaseReader;
        this.closeCount = new AtomicInteger(1);
    }

    @Override
    public Map<String, Object> getGeoData(final InetAddress inetAddress,
                                          final Collection<GeoIPField> fields,
                                          final Collection<GeoIPDatabase> geoIPDatabases) {
        return delegateDatabaseReader.getGeoData(inetAddress, fields, geoIPDatabases);
    }

    @Override
    public boolean isExpired() {
        return delegateDatabaseReader.isExpired();
    }

    @Override
    public void retain() {
        closeCount.incrementAndGet();
    }

    @Override
    public void close() throws Exception {
        final int count = closeCount.decrementAndGet();
        if (count == 0) {
            LOG.debug("Closing old geoip database readers");
            delegateDatabaseReader.close();
        }
    }
}
