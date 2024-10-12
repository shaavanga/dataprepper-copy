/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DistributionVersion;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OpenSearchSourceConfigurationTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void open_search_source_deprecated_username_password_only() throws JsonProcessingException {

        final String sourceConfigurationYaml =
                "hosts: [\"http://localhost:9200\"]\n" +
                "username: test\n" +
                "password: test\n" +
                "connection:\n" +
                "  insecure: true\n" +
                "  cert: \"cert\"\n" +
                "indices:\n" +
                "  include:\n" +
                "    - index_name_regex: \"regex\"\n" +
                "    - index_name_regex: \"regex-two\"\n" +
                "scheduling:\n" +
                "  index_read_count: 3\n" +
                "search_options:\n" +
                "  batch_size: 1000\n";
        final OpenSearchSourceConfiguration sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, OpenSearchSourceConfiguration.class);

        assertThat(sourceConfiguration.getSearchConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getConnectionConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getIndexParametersConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getSchedulingParameterConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getHosts(), notNullValue());

        sourceConfiguration.validateAuthConfigConflictWithDeprecatedUsernameAndPassword();
        sourceConfiguration.validateAwsConfigWithUsernameAndPassword();
        assertThat(sourceConfiguration.getPassword(), equalTo("test"));
        assertThat(sourceConfiguration.getUsername(), equalTo("test"));
        assertThat(sourceConfiguration.getAwsAuthenticationOptions(), equalTo(null));
    }

    @Test
    void open_search_source_authentication_only() throws JsonProcessingException {

        final String sourceConfigurationYaml =
                "hosts: [\"http://localhost:9200\"]\n" +
                        "authentication:\n" +
                        "  username: test\n" +
                        "  password: test\n" +
                        "connection:\n" +
                        "  insecure: true\n" +
                        "  cert: \"cert\"\n" +
                        "indices:\n" +
                        "  include:\n" +
                        "    - index_name_regex: \"regex\"\n" +
                        "    - index_name_regex: \"regex-two\"\n" +
                        "scheduling:\n" +
                        "  index_read_count: 3\n" +
                        "search_options:\n" +
                        "  batch_size: 1000\n";
        final OpenSearchSourceConfiguration sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, OpenSearchSourceConfiguration.class);

        assertThat(sourceConfiguration.getSearchConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getConnectionConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getIndexParametersConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getSchedulingParameterConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getHosts(), notNullValue());

        sourceConfiguration.validateAuthConfigConflictWithDeprecatedUsernameAndPassword();
        sourceConfiguration.validateAwsConfigWithUsernameAndPassword();
        assertThat(sourceConfiguration.getAuthConfig().getPassword(), equalTo("test"));
        assertThat(sourceConfiguration.getAuthConfig().getUsername(), equalTo("test"));
        assertThat(sourceConfiguration.getPassword(), nullValue());
        assertThat(sourceConfiguration.getUsername(), nullValue());
        assertThat(sourceConfiguration.getAwsAuthenticationOptions(), equalTo(null));
    }

    @Test
    void open_search_disabled_authentication() throws JsonProcessingException {

        final String sourceConfigurationYaml =
                "hosts: [\"http://localhost:9200\"]\n" +
                        "disable_authentication: true\n" +
                        "connection:\n" +
                        "  insecure: true\n" +
                        "  cert: \"cert\"\n" +
                        "indices:\n" +
                        "  include:\n" +
                        "    - index_name_regex: \"regex\"\n" +
                        "    - index_name_regex: \"regex-two\"\n" +
                        "scheduling:\n" +
                        "  index_read_count: 3\n" +
                        "search_options:\n" +
                        "  batch_size: 1000\n";
        final OpenSearchSourceConfiguration sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, OpenSearchSourceConfiguration.class);

        assertThat(sourceConfiguration.getSearchConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getConnectionConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getIndexParametersConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getSchedulingParameterConfiguration(), notNullValue());
        assertThat(sourceConfiguration.getHosts(), notNullValue());

        sourceConfiguration.validateAuthConfigConflictWithDeprecatedUsernameAndPassword();
        sourceConfiguration.validateAwsConfigWithUsernameAndPassword();
        assertThat(sourceConfiguration.isAuthenticationDisabled(), equalTo(true));
        assertThat(sourceConfiguration.getPassword(), equalTo(null));
        assertThat(sourceConfiguration.getUsername(), equalTo(null));
        assertThat(sourceConfiguration.getAwsAuthenticationOptions(), equalTo(null));
        assertThat(sourceConfiguration.getDistributionVersion(), nullValue());
    }

    @Test
    void opensearch_source_aws_only() throws JsonProcessingException {
        final String sourceConfigurationYaml = "hosts: [\"http://localhost:9200\"]\n" +
                "connection:\n" +
                "  insecure: true\n" +
                "  cert: \"cert\"\n" +
                "distribution_version: \"es7\"\n" +
                "indices:\n" +
                "  include:\n" +
                "    - index_name_regex: \"regex\"\n" +
                "    - index_name_regex: \"regex-two\"\n" +
                "aws:\n" +
                "  region: \"us-east-1\"\n" +
                "  sts_role_arn: \"arn:aws:iam::123456789012:role/aos-role\"\n" +
                "scheduling:\n" +
                "  index_read_count: 3\n" +
                "search_options:\n" +
                "  batch_size: 1000\n";

        final OpenSearchSourceConfiguration sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, OpenSearchSourceConfiguration.class);

        sourceConfiguration.validateAuthConfigConflictWithDeprecatedUsernameAndPassword();
        sourceConfiguration.validateAwsConfigWithUsernameAndPassword();
        assertThat(sourceConfiguration.getPassword(), equalTo(null));
        assertThat(sourceConfiguration.getUsername(), equalTo(null));
        assertThat(sourceConfiguration.getAwsAuthenticationOptions(), notNullValue());

        assertThat(sourceConfiguration.getAwsAuthenticationOptions().getAwsStsRoleArn(),
            equalTo("arn:aws:iam::123456789012:role/aos-role"));

        assertThat(sourceConfiguration.isDistributionVersionValid(), equalTo(true));
        assertThat(sourceConfiguration.getDistributionVersion(), equalTo(DistributionVersion.ES7));
    }

    @Test
    void opensearch_source_aws_sts_external_id() throws JsonProcessingException {
        final String sourceConfigurationYaml = "hosts: [\"http://localhost:9200\"]\n" +
            "connection:\n" +
            "  insecure: true\n" +
            "  cert: \"cert\"\n" +
            "distribution_version: \"opensearch\"\n" +
            "indices:\n" +
            "  include:\n" +
            "    - index_name_regex: \"regex\"\n" +
            "    - index_name_regex: \"regex-two\"\n" +
            "aws:\n" +
            "  region: \"us-east-1\"\n" +
            "  sts_role_arn: \"arn:aws:iam::123456789012:role/aos-role\"\n" +
            "  sts_external_id: \"some-random-id\"\n" +
            "scheduling:\n" +
            "  index_read_count: 3\n" +
            "search_options:\n" +
            "  batch_size: 1000\n";

        final OpenSearchSourceConfiguration sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, OpenSearchSourceConfiguration.class);

        sourceConfiguration.validateAuthConfigConflictWithDeprecatedUsernameAndPassword();
        sourceConfiguration.validateAwsConfigWithUsernameAndPassword();
        assertThat(sourceConfiguration.getPassword(), equalTo(null));
        assertThat(sourceConfiguration.getUsername(), equalTo(null));
        assertThat(sourceConfiguration.getAwsAuthenticationOptions(), notNullValue());

        assertThat(sourceConfiguration.getAwsAuthenticationOptions().getAwsStsRoleArn(),
            equalTo("arn:aws:iam::123456789012:role/aos-role"));
        assertThat(sourceConfiguration.getAwsAuthenticationOptions().getAwsStsExternalId(),
            equalTo("some-random-id"));

        assertThat(sourceConfiguration.isDistributionVersionValid(), equalTo(true));
        assertThat(sourceConfiguration.getDistributionVersion(), equalTo(DistributionVersion.OPENSEARCH));
    }

    @Test
    void using_both_aws_config_and_username_password_is_invalid() throws JsonProcessingException {
        final String sourceConfigurationYaml =
                "hosts: [\"http://localhost:9200\"]\n" +
                "username: test\n" +
                "password: test\n" +
                "connection:\n" +
                "  insecure: true\n" +
                "  cert: \"cert\"\n" +
                "distribution_version: \"invalid\"\n" +
                "indices:\n" +
                "  include:\n" +
                "    - index_name_regex: \"regex\"\n" +
                "    - index_name_regex: \"regex-two\"\n" +
                "aws:\n" +
                "  region: \"us-east-1\"\n" +
                "  sts_role_arn: \"arn:aws:iam::123456789012:role/aos-role\"\n" +
                "scheduling:\n" +
                "  index_read_count: 3\n" +
                "search_options:\n" +
                "  batch_size: 1000\n";

        final OpenSearchSourceConfiguration sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, OpenSearchSourceConfiguration.class);

        sourceConfiguration.validateAuthConfigConflictWithDeprecatedUsernameAndPassword();
        assertThrows(InvalidPluginConfigurationException.class, sourceConfiguration::validateAwsConfigWithUsernameAndPassword);
        assertThat(sourceConfiguration.isDistributionVersionValid(), equalTo(false));
    }

    @Test
    void one_of_username_password_or_aws_config_or_authDisabled_is_required() throws JsonProcessingException {
        final String sourceConfigurationYaml =
                        "hosts: [\"http://localhost:9200\"]\n" +
                        "connection:\n" +
                        "  insecure: true\n" +
                        "  cert: \"cert\"\n" +
                        "indices:\n" +
                        "  include:\n" +
                        "    - index_name_regex: \"regex\"\n" +
                        "    - index_name_regex: \"regex-two\"\n" +
                        "scheduling:\n" +
                        "  index_read_count: 3\n" +
                        "search_options:\n" +
                        "  batch_size: 1000\n";

        final OpenSearchSourceConfiguration sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, OpenSearchSourceConfiguration.class);

        sourceConfiguration.validateAuthConfigConflictWithDeprecatedUsernameAndPassword();
        assertThrows(InvalidPluginConfigurationException.class, sourceConfiguration::validateAwsConfigWithUsernameAndPassword);
    }

    @Test
    void using_both_deprecated_username_password_and_authentication_is_invalid() throws JsonProcessingException {
        final String sourceConfigurationYaml =
                "hosts: [\"http://localhost:9200\"]\n" +
                        "username: test\n" +
                        "password: test\n" +
                        "connection:\n" +
                        "  insecure: true\n" +
                        "  cert: \"cert\"\n" +
                        "distribution_version: \"invalid\"\n" +
                        "indices:\n" +
                        "  include:\n" +
                        "    - index_name_regex: \"regex\"\n" +
                        "    - index_name_regex: \"regex-two\"\n" +
                        "authentication:\n" +
                        "  username: \"test\"\n" +
                        "  password: \"test\"\n" +
                        "scheduling:\n" +
                        "  index_read_count: 3\n" +
                        "search_options:\n" +
                        "  batch_size: 1000\n";

        final OpenSearchSourceConfiguration sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, OpenSearchSourceConfiguration.class);

        assertThrows(InvalidPluginConfigurationException.class, sourceConfiguration::validateAuthConfigConflictWithDeprecatedUsernameAndPassword);
        assertThat(sourceConfiguration.isDistributionVersionValid(), equalTo(false));
    }
}
