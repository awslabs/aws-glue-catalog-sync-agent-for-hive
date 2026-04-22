package com.amazonaws.services.glue.catalog;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory for creating AWS Glue clients from Hadoop Configuration.
 * Reads the region from {@code glue.catalog.region} (default: us-east-1)
 * and uses the default AWS credential provider chain.
 *
 * Retry behaviour for transient errors (429, 500, 503) is handled by the
 * SDK's built-in retry policy configured here, so callers do not need to
 * implement their own retry loops for those cases.
 */
public class GlueClientFactory {

    static final String GLUE_CATALOG_REGION = "glue.catalog.region";
    static final String DEFAULT_REGION = "us-east-1";

    private static final String RETRY_MAX_ATTEMPTS = "glue.catalog.retry.maxAttempts";
    private static final int DEFAULT_MAX_ATTEMPTS = 5;

    public static AWSGlue createClient(Configuration config) {
        String region = config.get(GLUE_CATALOG_REGION, DEFAULT_REGION);
        int maxRetries = config.getInt(RETRY_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS);

        RetryPolicy retryPolicy = new RetryPolicy(
                PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
                maxRetries,
                true);

        ClientConfiguration clientConfig = new ClientConfiguration()
                .withRetryPolicy(retryPolicy);

        return AWSGlueClientBuilder.standard()
                .withRegion(region)
                .withClientConfiguration(clientConfig)
                .build();
    }
}
