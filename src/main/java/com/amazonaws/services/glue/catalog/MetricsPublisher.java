package com.amazonaws.services.glue.catalog;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

/**
 * Collects {@link MetricDatum} objects in a thread-safe buffer and publishes
 * them to CloudWatch at a configurable interval.
 * <p>
 * Thread safety: Uses {@link ConcurrentLinkedQueue} for the buffer (lock-free).
 * The flush thread is the only consumer of the buffer.
 * </p>
 */
public class MetricsPublisher implements MetricsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsPublisher.class);

    static final String METRICS_NAMESPACE_KEY = "glue.catalog.metrics.namespace";
    static final String METRICS_PUBLISH_INTERVAL_KEY = "glue.catalog.metrics.publish.interval.seconds";
    static final String DEFAULT_NAMESPACE = "HiveGlueCatalogSync";
    static final int DEFAULT_PUBLISH_INTERVAL_SECONDS = 60;
    static final int CW_MAX_DATAPOINTS_PER_REQUEST = 1000;

    private final AmazonCloudWatch cloudWatchClient;
    private final String namespace;
    private final ConcurrentLinkedQueue<MetricDatum> buffer;
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> flushTask;

    public MetricsPublisher(Configuration config) {
        this(config, buildCloudWatchClient(config));
    }

    /**
     * Package-private constructor for testing — allows injecting a mock CloudWatch client.
     */
    MetricsPublisher(Configuration config, AmazonCloudWatch cloudWatchClient) {
        this.cloudWatchClient = cloudWatchClient;
        this.namespace = config.get(METRICS_NAMESPACE_KEY, DEFAULT_NAMESPACE);
        int publishIntervalSeconds = config.getInt(METRICS_PUBLISH_INTERVAL_KEY, DEFAULT_PUBLISH_INTERVAL_SECONDS);
        this.buffer = new ConcurrentLinkedQueue<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "metrics-publisher-flush");
                t.setDaemon(true);
                return t;
            }
        });
        this.flushTask = scheduler.scheduleAtFixedRate(
                this::flush,
                publishIntervalSeconds,
                publishIntervalSeconds,
                TimeUnit.SECONDS
        );
    }

    private static AmazonCloudWatch buildCloudWatchClient(Configuration config) {
        String region = config.get(GlueClientFactory.GLUE_CATALOG_REGION, GlueClientFactory.DEFAULT_REGION);
        return AmazonCloudWatchClientBuilder.standard()
                .withRegion(region)
                .build();
    }

    // -- Accessor methods for testing --

    String getNamespace() {
        return namespace;
    }

    ConcurrentLinkedQueue<MetricDatum> getBuffer() {
        return buffer;
    }

    @Override
    public void recordQueueDepth(int depth) {
        buffer.add(new MetricDatum()
                .withMetricName("QueueDepth")
                .withValue((double) depth)
                .withUnit(StandardUnit.Count)
                .withTimestamp(new Date()));
    }

    @Override
    public void recordBatchSize(int size) {
        buffer.add(new MetricDatum()
                .withMetricName("BatchSize")
                .withValue((double) size)
                .withUnit(StandardUnit.Count)
                .withTimestamp(new Date()));
    }

    @Override
    public void recordSyncLagMs(long lagMs) {
        buffer.add(new MetricDatum()
                .withMetricName("SyncLagMs")
                .withValue((double) lagMs)
                .withUnit(StandardUnit.Milliseconds)
                .withTimestamp(new Date()));
    }

    @Override
    public void recordOperationSuccess(CatalogOperation.OperationType type) {
        buffer.add(new MetricDatum()
                .withMetricName("OperationSuccess")
                .withValue(1.0)
                .withUnit(StandardUnit.Count)
                .withTimestamp(new Date())
                .withDimensions(operationTypeDimension(type)));
    }

    @Override
    public void recordOperationFailure(CatalogOperation.OperationType type) {
        buffer.add(new MetricDatum()
                .withMetricName("OperationFailure")
                .withValue(1.0)
                .withUnit(StandardUnit.Count)
                .withTimestamp(new Date())
                .withDimensions(operationTypeDimension(type)));
    }

    @Override
    public void recordRetryCount(CatalogOperation.OperationType type, int count) {
        buffer.add(new MetricDatum()
                .withMetricName("RetryCount")
                .withValue((double) count)
                .withUnit(StandardUnit.Count)
                .withTimestamp(new Date())
                .withDimensions(operationTypeDimension(type)));
    }

    @Override
    public void recordBatchProcessingTimeMs(long timeMs) {
        buffer.add(new MetricDatum()
                .withMetricName("BatchProcessingTimeMs")
                .withValue((double) timeMs)
                .withUnit(StandardUnit.Milliseconds)
                .withTimestamp(new Date()));
    }

    @Override
    public void recordThrottleCount() {
        buffer.add(new MetricDatum()
                .withMetricName("ThrottleCount")
                .withValue(1.0)
                .withUnit(StandardUnit.Count)
                .withTimestamp(new Date()));
    }
    @Override
    public void recordQueueRejection(CatalogOperation.OperationType type) {
        buffer.add(new MetricDatum()
                .withMetricName("QueueRejection")
                .withValue(1.0)
                .withUnit(StandardUnit.Count)
                .withTimestamp(new Date())
                .withDimensions(operationTypeDimension(type)));
    }


    @Override
    public void flush() {
        List<MetricDatum> batch = new ArrayList<>();
        MetricDatum datum;
        while ((datum = buffer.poll()) != null) {
            batch.add(datum);
            if (batch.size() == CW_MAX_DATAPOINTS_PER_REQUEST) {
                publishBatch(batch);
                batch = new ArrayList<>();
            }
        }
        if (!batch.isEmpty()) {
            publishBatch(batch);
        }
    }

    @Override
    public void shutdown() {
        flushTask.cancel(false);
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flush();
    }

    private void publishBatch(List<MetricDatum> batch) {
        try {
            PutMetricDataRequest request = new PutMetricDataRequest()
                    .withNamespace(namespace)
                    .withMetricData(batch);
            cloudWatchClient.putMetricData(request);
        } catch (Exception e) {
            LOG.warn("Failed to publish {} metric data points to CloudWatch namespace '{}'. "
                    + "Discarding batch.", batch.size(), namespace, e);
        }
    }

    private static Dimension operationTypeDimension(CatalogOperation.OperationType type) {
        return new Dimension()
                .withName("OperationType")
                .withValue(type.name());
    }
}
