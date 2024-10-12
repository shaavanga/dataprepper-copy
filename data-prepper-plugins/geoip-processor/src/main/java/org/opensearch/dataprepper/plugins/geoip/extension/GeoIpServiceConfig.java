/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import jakarta.validation.Valid;

public class GeoIpServiceConfig {
    private static final MaxMindConfig DEFAULT_MAXMIND_CONFIG = new MaxMindConfig();

    @Valid
    @JsonProperty("maxmind")
    @JsonSetter(nulls = Nulls.SKIP)
    private MaxMindConfig maxMindConfig = DEFAULT_MAXMIND_CONFIG;

    /**
     * Gets the configuration for MaxMind.
     *
     * @return The MaxMind configuration
     * @since 2.7
     */
    public MaxMindConfig getMaxMindConfig() {
        return maxMindConfig;
    }
}
