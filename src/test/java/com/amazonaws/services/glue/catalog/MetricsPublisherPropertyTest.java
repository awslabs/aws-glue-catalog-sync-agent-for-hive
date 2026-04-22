package com.amazonaws.services.glue.catalog;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;

/**
 * Property-based tests for MetricsPublisher and MetricsCollectorFactory.
 *
 * Feature: load-testing-and-metrics
 */
public class MetricsPublisherPropertyTest {

    // ---- Generators ----

    @Provide
    Arbitrary<String> namespaces() {
        return Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1)
            .ofMaxLength(50);
    }

    @Provide
    Arbitrary<Integer> positiveIntervals() {
        return Arbitraries.integers().between(1, 3600);
    }

    @Provide
    Arbitrary<Integer> metricCounts() {
        return Arbitraries.integers().between(0, 3000);
    }

    // ---- Property 1: Factory returns correct implementation based on config ----

    // Feature: load-testing-and-metrics, Property 1: Factory returns correct implementation based on config
    /**
     * Validates: Requirements 1.1, 1.2
     * When metrics are disabled (false or absent), the factory always returns NoOpMetricsCollector.
     */
    @Property(tries = 100)
    void factoryReturnsNoOpWhenDisabled(@ForAll("namespaces") String namespace) {
        Configuration config = new Configuration(false);
        config.setBoolean(MetricsCollectorFactory.METRICS_ENABLED, false);
        config.set(MetricsCollectorFactory.METRICS_NAMESPACE, namespace);

        MetricsCollector collector = MetricsCollectorFactory.create(config);
        assert collector instanceof NoOpMetricsCollector
            : "Expected NoOpMetricsCollector when disabled, got " + collector.getClass().getSimpleName();
    }

    // Feature: load-testing-and-metrics, Property 1: Factory returns correct implementation based on config
    /**
     * Validates: Requirements 1.1, 1.2
     * When metrics.enabled is absent from config, the factory returns NoOpMetricsCollector.
     */
    @Property(tries = 100)
    void factoryReturnsNoOpWhenAbsent(@ForAll("namespaces") String namespace) {
        Configuration config = new Configuration(false);
        // Do not set METRICS_ENABLED at all
        config.set(MetricsCollectorFactory.METRICS_NAMESPACE, namespace);

        MetricsCollector collector = MetricsCollectorFactory.create(config);
        assert collector instanceof NoOpMetricsCollector
            : "Expected NoOpMetricsCollector when enabled is absent, got " + collector.getClass().getSimpleName();
    }

    // Feature: load-testing-and-metrics, Property 1: Factory returns correct implementation based on config
    /**
     * Validates: Requirements 1.1, 1.2
     * When metrics are enabled, the factory never throws and always returns a valid
     * MetricsCollector — either MetricsPublisher (if CloudWatch client creation succeeds)
     * or NoOpMetricsCollector (if it fails and the factory falls back).
     */
    @Property(tries = 100)
    void factoryNeverThrowsAndReturnsValidCollectorWhenEnabled(@ForAll("namespaces") String namespace) {
        Configuration config = new Configuration(false);
        config.setBoolean(MetricsCollectorFactory.METRICS_ENABLED, true);
        config.set(MetricsCollectorFactory.METRICS_NAMESPACE, namespace);

        MetricsCollector collector = MetricsCollectorFactory.create(config);
        assert collector != null
            : "Factory should never return null";
        assert (collector instanceof MetricsPublisher || collector instanceof NoOpMetricsCollector)
            : "Factory should return MetricsPublisher or NoOpMetricsCollector, got "
              + collector.getClass().getSimpleName();

        // Clean up if a real MetricsPublisher was created (has a scheduler thread)
        if (collector instanceof MetricsPublisher) {
            collector.shutdown();
        }
    }

    // ---- Property 2: Namespace and interval configuration pass-through ----

    // Feature: load-testing-and-metrics, Property 2: Namespace and interval configuration pass-through
    /**
     * Validates: Requirements 1.3, 1.4, 3.6
     * For any non-empty namespace string, when set in Hadoop Configuration,
     * the MetricsPublisher uses that namespace.
     */
    @Property(tries = 100)
    void namespacePassesThrough(@ForAll("namespaces") String namespace) {
        Configuration config = new Configuration(false);
        config.set(MetricsPublisher.METRICS_NAMESPACE_KEY, namespace);
        config.setInt(MetricsPublisher.METRICS_PUBLISH_INTERVAL_KEY, 60);

        AmazonCloudWatch mockClient = mock(AmazonCloudWatch.class);
        MetricsPublisher publisher = new MetricsPublisher(config, mockClient);
        try {
            assert namespace.equals(publisher.getNamespace())
                : "Expected namespace '" + namespace + "' but got '" + publisher.getNamespace() + "'";
        } finally {
            publisher.shutdown();
        }
    }

    // Feature: load-testing-and-metrics, Property 2: Namespace and interval configuration pass-through
    /**
     * Validates: Requirements 1.3, 1.4, 3.6
     * When namespace is absent from config, the default "HiveGlueCatalogSync" is used.
     */
    @Property(tries = 100)
    void namespaceDefaultsWhenAbsent(@ForAll("positiveIntervals") int interval) {
        Configuration config = new Configuration(false);
        // Do not set namespace
        config.setInt(MetricsPublisher.METRICS_PUBLISH_INTERVAL_KEY, interval);

        AmazonCloudWatch mockClient = mock(AmazonCloudWatch.class);
        MetricsPublisher publisher = new MetricsPublisher(config, mockClient);
        try {
            assert MetricsPublisher.DEFAULT_NAMESPACE.equals(publisher.getNamespace())
                : "Expected default namespace '" + MetricsPublisher.DEFAULT_NAMESPACE
                  + "' but got '" + publisher.getNamespace() + "'";
        } finally {
            publisher.shutdown();
        }
    }

    // ---- Property 7: Flush batches metrics into correct number of API calls ----

    // Feature: load-testing-and-metrics, Property 7: Flush batches metrics into correct number of API calls
    /**
     * Validates: Requirements 3.1, 3.3
     * For any number N of buffered MetricDatum objects, calling flush() results in
     * exactly ceil(N / 1000) PutMetricData API calls, each containing at most 1000
     * data points, and the buffer is empty afterward.
     */
    @Property(tries = 100)
    void flushBatchesCorrectly(@ForAll("metricCounts") int n) {
        Configuration config = new Configuration(false);
        config.setInt(MetricsPublisher.METRICS_PUBLISH_INTERVAL_KEY, 3600);

        AmazonCloudWatch mockClient = mock(AmazonCloudWatch.class);
        when(mockClient.putMetricData(any(PutMetricDataRequest.class)))
            .thenReturn(new PutMetricDataResult());

        MetricsPublisher publisher = new MetricsPublisher(config, mockClient);
        try {
            // Add N MetricDatum objects directly to the buffer
            ConcurrentLinkedQueue<MetricDatum> buffer = publisher.getBuffer();
            for (int i = 0; i < n; i++) {
                buffer.add(new MetricDatum()
                    .withMetricName("TestMetric")
                    .withValue((double) i)
                    .withUnit(StandardUnit.Count)
                    .withTimestamp(new Date()));
            }

            publisher.flush();

            // Verify correct number of API calls
            int expectedCalls = n == 0 ? 0 : (int) Math.ceil((double) n / 1000);
            ArgumentCaptor<PutMetricDataRequest> captor = ArgumentCaptor.forClass(PutMetricDataRequest.class);

            if (expectedCalls == 0) {
                verify(mockClient, times(0)).putMetricData(any(PutMetricDataRequest.class));
            } else {
                verify(mockClient, times(expectedCalls)).putMetricData(captor.capture());

                // Verify each call has at most 1000 data points
                List<PutMetricDataRequest> requests = captor.getAllValues();
                int totalDataPoints = 0;
                for (PutMetricDataRequest request : requests) {
                    int batchSize = request.getMetricData().size();
                    assert batchSize <= 1000
                        : "Batch size " + batchSize + " exceeds max of 1000";
                    assert batchSize > 0
                        : "Batch should not be empty";
                    totalDataPoints += batchSize;
                }
                assert totalDataPoints == n
                    : "Total data points published (" + totalDataPoints + ") != buffered (" + n + ")";
            }

            // Verify buffer is empty afterward
            assert buffer.isEmpty()
                : "Buffer should be empty after flush, but has " + buffer.size() + " items";
        } finally {
            publisher.shutdown();
        }
    }

    // ---- Property 8: Shutdown flushes all remaining metrics ----

    // Feature: load-testing-and-metrics, Property 8: Shutdown flushes all remaining metrics
    /**
     * Validates: Requirements 3.4
     * For any set of buffered metrics, calling shutdown() results in all buffered
     * metrics being published and the buffer is empty afterward.
     */
    @Property(tries = 100)
    void shutdownFlushesAll(@ForAll("metricCounts") int n) {
        Configuration config = new Configuration(false);
        config.setInt(MetricsPublisher.METRICS_PUBLISH_INTERVAL_KEY, 3600);

        AmazonCloudWatch mockClient = mock(AmazonCloudWatch.class);
        when(mockClient.putMetricData(any(PutMetricDataRequest.class)))
            .thenReturn(new PutMetricDataResult());

        MetricsPublisher publisher = new MetricsPublisher(config, mockClient);

        // Add N MetricDatum objects directly to the buffer
        ConcurrentLinkedQueue<MetricDatum> buffer = publisher.getBuffer();
        for (int i = 0; i < n; i++) {
            buffer.add(new MetricDatum()
                .withMetricName("TestMetric")
                .withValue((double) i)
                .withUnit(StandardUnit.Count)
                .withTimestamp(new Date()));
        }

        // Call shutdown instead of flush
        publisher.shutdown();

        // Verify all metrics were published
        int expectedCalls = n == 0 ? 0 : (int) Math.ceil((double) n / 1000);
        ArgumentCaptor<PutMetricDataRequest> captor = ArgumentCaptor.forClass(PutMetricDataRequest.class);

        if (expectedCalls == 0) {
            verify(mockClient, times(0)).putMetricData(any(PutMetricDataRequest.class));
        } else {
            verify(mockClient, times(expectedCalls)).putMetricData(captor.capture());

            // Verify total data points match
            int totalDataPoints = 0;
            for (PutMetricDataRequest request : captor.getAllValues()) {
                totalDataPoints += request.getMetricData().size();
            }
            assert totalDataPoints == n
                : "Total data points published (" + totalDataPoints + ") != buffered (" + n + ")";
        }

        // Verify buffer is empty afterward
        assert buffer.isEmpty()
            : "Buffer should be empty after shutdown, but has " + buffer.size() + " items";
    }
}
