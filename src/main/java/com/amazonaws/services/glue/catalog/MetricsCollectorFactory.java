package com.amazonaws.services.glue.catalog;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that reads Hadoop configuration and returns the appropriate
 * {@link MetricsCollector} implementation.
 */
public class MetricsCollectorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsCollectorFactory.class);

    static final String METRICS_ENABLED = "glue.catalog.metrics.enabled";
    static final String METRICS_NAMESPACE = "glue.catalog.metrics.namespace";
    static final String METRICS_PUBLISH_INTERVAL = "glue.catalog.metrics.publish.interval.seconds";
    static final String DEFAULT_NAMESPACE = "HiveGlueCatalogSync";
    static final int DEFAULT_PUBLISH_INTERVAL = 60;

    /**
     * Creates a {@link MetricsCollector} based on the Hadoop configuration.
     *
     * @param config Hadoop configuration
     * @return {@link MetricsPublisher} when enabled, {@link NoOpMetricsCollector} when disabled
     */
    public static MetricsCollector create(Configuration config) {
        boolean enabled = config.getBoolean(METRICS_ENABLED, false);
        if (!enabled) {
            LOG.info("Metrics collection is disabled. Using NoOpMetricsCollector.");
            return new NoOpMetricsCollector();
        }
        try {
            LOG.info("Metrics collection is enabled. Creating MetricsPublisher.");
            return new MetricsPublisher(config);
        } catch (Exception e) {
            LOG.warn("Failed to create MetricsPublisher. Falling back to NoOpMetricsCollector.", e);
            return new NoOpMetricsCollector();
        }
    }
}
