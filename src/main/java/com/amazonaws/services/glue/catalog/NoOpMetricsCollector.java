package com.amazonaws.services.glue.catalog;

/**
 * Null Object implementation of {@link MetricsCollector}.
 * <p>
 * All methods are empty no-ops. Used when metrics are disabled to avoid
 * conditional checks in the hot path, ensuring zero runtime overhead.
 * </p>
 */
public class NoOpMetricsCollector implements MetricsCollector {

    @Override
    public void recordQueueDepth(int depth) { }

    @Override
    public void recordBatchSize(int size) { }

    @Override
    public void recordSyncLagMs(long lagMs) { }

    @Override
    public void recordOperationSuccess(CatalogOperation.OperationType type) { }

    @Override
    public void recordOperationFailure(CatalogOperation.OperationType type) { }

    @Override
    public void recordRetryCount(CatalogOperation.OperationType type, int count) { }

    @Override
    public void recordBatchProcessingTimeMs(long timeMs) { }

    @Override
    public void recordThrottleCount() { }
    @Override
    public void recordQueueRejection(CatalogOperation.OperationType type) { }


    @Override
    public void flush() { }

    @Override
    public void shutdown() { }
}
