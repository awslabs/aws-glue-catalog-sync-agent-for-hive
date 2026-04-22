package com.amazonaws.services.glue.catalog;

/**
 * Contract for recording sync agent operational metrics.
 * <p>
 * Implementations must be thread-safe as methods may be called from the
 * GlueCatalogQueueProcessor thread concurrently with the flush/publish thread.
 * </p>
 */
public interface MetricsCollector {

    /**
     * Records the current queue depth before a drain cycle begins.
     *
     * @param depth the number of operations in the queue
     */
    void recordQueueDepth(int depth);

    /**
     * Records the number of operations drained in a single batch.
     *
     * @param size the batch size
     */
    void recordBatchSize(int size);

    /**
     * Records the sync lag in milliseconds between enqueue and execution completion.
     *
     * @param lagMs elapsed time in milliseconds
     */
    void recordSyncLagMs(long lagMs);

    /**
     * Records a successful Glue API call.
     *
     * @param type the operation type that succeeded
     */
    void recordOperationSuccess(CatalogOperation.OperationType type);

    /**
     * Records a permanently failed Glue API call (after all retries exhausted).
     *
     * @param type the operation type that failed
     */
    void recordOperationFailure(CatalogOperation.OperationType type);

    /**
     * Records the number of retry attempts for an operation.
     *
     * @param type  the operation type
     * @param count the total number of retry attempts
     */
    void recordRetryCount(CatalogOperation.OperationType type, int count);

    /**
     * Records the wall-clock time to process an entire batch.
     *
     * @param timeMs elapsed time in milliseconds
     */
    void recordBatchProcessingTimeMs(long timeMs);

    /**
     * Records a throttle event (HTTP 429) from the Glue API.
     */
    void recordThrottleCount();
    /**
     * Records a queue rejection event when the bounded queue is full.
     *
     * @param type the operation type that was rejected
     */
    void recordQueueRejection(CatalogOperation.OperationType type);


    /**
     * Flushes all buffered metrics to the underlying metrics service.
     */
    void flush();

    /**
     * Shuts down the metrics collector, flushing any remaining buffered metrics.
     */
    void shutdown();
}
