package com.amazonaws.services.glue.catalog;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Property-based tests for GlueCatalogQueueProcessor metrics instrumentation.
 *
 * Feature: load-testing-and-metrics
 */
public class QueueProcessorMetricsPropertyTest {

    // ---- Reflection helpers ----

    @SuppressWarnings("unchecked")
    private LinkedBlockingQueue<CatalogOperation> getOperationQueue(HiveGlueCatalogSyncAgent agent) throws Exception {
        Field field = HiveGlueCatalogSyncAgent.class.getDeclaredField("operationQueue");
        field.setAccessible(true);
        return (LinkedBlockingQueue<CatalogOperation>) field.get(agent);
    }

    private Object createProcessor(HiveGlueCatalogSyncAgent agent, AWSGlue mockGlue,
            CloudWatchLogsReporter mockCwlr, MetricsCollector metricsCollector,
            int batchWindowMs) throws Exception {
        Class<?> processorClass = Class.forName(
            "com.amazonaws.services.glue.catalog.HiveGlueCatalogSyncAgent$GlueCatalogQueueProcessor");
        Constructor<?> ctor = processorClass.getDeclaredConstructor(
            HiveGlueCatalogSyncAgent.class,
            AWSGlue.class, CloudWatchLogsReporter.class,
            MetricsCollector.class,
            String.class, boolean.class, boolean.class,
            int.class);
        ctor.setAccessible(true);
        return ctor.newInstance(agent, mockGlue, mockCwlr,
            metricsCollector,
            null, false, false,
            batchWindowMs);
    }

    private void invokeExecuteOperation(Object processor, CatalogOperation op) throws Exception {
        Method method = processor.getClass().getDeclaredMethod("executeOperation", CatalogOperation.class);
        method.setAccessible(true);
        method.invoke(processor, op);
    }

    private void stopProcessor(Object processor) throws Exception {
        Method method = processor.getClass().getDeclaredMethod("stop");
        method.setAccessible(true);
        method.invoke(processor);
    }

    private TableInput buildMinimalTableInput(String tableName) {
        return new TableInput()
            .withName(tableName)
            .withTableType("EXTERNAL_TABLE")
            .withStorageDescriptor(new StorageDescriptor()
                .withInputFormat("org.apache.hadoop.mapred.TextInputFormat")
                .withOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .withLocation("s3://bucket/path")
                .withColumns(Collections.singletonList(new Column().withName("col1").withType("string")))
                .withSerdeInfo(new SerDeInfo()
                    .withSerializationLibrary("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")));
    }

    // ---- Generators ----

    @Provide
    Arbitrary<Integer> queueSizes() {
        return Arbitraries.integers().between(0, 200);
    }

    @Provide
    Arbitrary<CatalogOperation.OperationType> operationTypes() {
        return Arbitraries.of(CatalogOperation.OperationType.values());
    }


    // ---- Property 3: Queue depth and batch size recording ----

    // Feature: load-testing-and-metrics, Property 3: Queue depth and batch size recording
    /**
     * Validates: Requirements 2.1, 2.2
     *
     * For any non-negative number of CatalogOperations enqueued before a drain cycle,
     * the MetricsCollector records a QueueDepth metric equal to the queue size before
     * draining and a BatchSize metric equal to the number of operations actually drained.
     */
    @Property(tries = 100)
    void queueDepthAndBatchSizeRecordedCorrectly(@ForAll("queueSizes") int n) throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        MetricsCollector mockMetrics = mock(MetricsCollector.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        // Initialize the operationQueue on the agent
        LinkedBlockingQueue<CatalogOperation> queue = getOperationQueue(agent);
        if (queue == null) {
            Field field = HiveGlueCatalogSyncAgent.class.getDeclaredField("operationQueue");
            field.setAccessible(true);
            field.set(agent, new LinkedBlockingQueue<>());
            queue = getOperationQueue(agent);
        }

        // Enqueue N CREATE_TABLE operations (simplest type that succeeds with mock)
        for (int i = 0; i < n; i++) {
            queue.add(new CatalogOperation(
                CatalogOperation.OperationType.CREATE_TABLE,
                "testdb", "table_" + i,
                buildMinimalTableInput("table_" + i)));
        }

        // Create processor with very short batch window (1ms)
        Object processor = createProcessor(agent, mockGlue, mockCwlr, mockMetrics, 1);

        // Run the processor in a background thread so it does at least one drain cycle
        Thread processorThread = new Thread((Runnable) processor);
        processorThread.start();

        // Wait enough time for at least one drain cycle to complete
        Thread.sleep(50);

        // Stop the processor and wait for it to finish
        stopProcessor(processor);
        processorThread.join(2000);

        // Verify QueueDepth was recorded with the queue size before draining.
        // The processor records queue depth at the start of each cycle.
        // With n operations enqueued before the first cycle, the first recordQueueDepth(n) call
        // captures the pre-drain state. Subsequent cycles may record 0.
        verify(mockMetrics, atLeastOnce()).recordQueueDepth(anyInt());

        // The first call should have recorded the initial queue depth
        // Use ArgumentCaptor to verify the first recorded depth matches n
        org.mockito.ArgumentCaptor<Integer> depthCaptor = org.mockito.ArgumentCaptor.forClass(Integer.class);
        verify(mockMetrics, atLeastOnce()).recordQueueDepth(depthCaptor.capture());
        int firstRecordedDepth = depthCaptor.getAllValues().get(0);
        assert firstRecordedDepth == n
            : "First recorded QueueDepth should be " + n + " but was " + firstRecordedDepth;

        // Verify BatchSize was recorded with the number of operations drained
        if (n > 0) {
            org.mockito.ArgumentCaptor<Integer> batchCaptor = org.mockito.ArgumentCaptor.forClass(Integer.class);
            verify(mockMetrics, atLeastOnce()).recordBatchSize(batchCaptor.capture());
            int firstRecordedBatch = batchCaptor.getAllValues().get(0);
            assert firstRecordedBatch == n
                : "First recorded BatchSize should be " + n + " but was " + firstRecordedBatch;
        } else {
            verify(mockMetrics, never()).recordBatchSize(anyInt());
        }
    }


    // ---- Property 5: Operation outcome metrics record correct type dimension ----

    // Feature: load-testing-and-metrics, Property 5: Operation outcome metrics record correct type dimension
    /**
     * Validates: Requirements 2.4, 2.5, 2.6, 2.8
     *
     * For any CatalogOperation of any OperationType, when the Glue API call succeeds,
     * an OperationSuccess metric with the correct OperationType is recorded; when it
     * fails permanently, an OperationFailure metric is recorded.
     */
    @Property(tries = 100)
    void operationSuccessRecordsCorrectType(@ForAll("operationTypes") CatalogOperation.OperationType type)
            throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        MetricsCollector mockMetrics = mock(MetricsCollector.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        Object processor = createProcessor(agent, mockGlue, mockCwlr, mockMetrics, 100);

        // Build an appropriate operation for the type
        CatalogOperation op = buildOperationForType(type);

        invokeExecuteOperation(processor, op);

        // Verify OperationSuccess was recorded with the correct type
        verify(mockMetrics).recordOperationSuccess(type);
        verify(mockMetrics, never()).recordOperationFailure(any());
    }

    // Feature: load-testing-and-metrics, Property 5: Operation outcome metrics record correct type dimension
    /**
     * Validates: Requirements 2.4, 2.5, 2.6, 2.8
     *
     * For any CatalogOperation of any OperationType, when the Glue API call fails
     * permanently (non-transient error, no retries), an OperationFailure metric with
     * the correct OperationType is recorded.
     */
    @Property(tries = 100)
    void operationFailureRecordsCorrectType(@ForAll("operationTypes") CatalogOperation.OperationType type)
            throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        MetricsCollector mockMetrics = mock(MetricsCollector.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        // Configure mock to throw a non-transient error (400) for all API calls
        AmazonServiceException badRequest = new AmazonServiceException("Invalid input");
        badRequest.setStatusCode(400);
        when(mockGlue.createTable(any(CreateTableRequest.class))).thenThrow(badRequest);
        when(mockGlue.deleteTable(any(DeleteTableRequest.class))).thenThrow(badRequest);
        when(mockGlue.batchCreatePartition(any(BatchCreatePartitionRequest.class))).thenThrow(badRequest);
        when(mockGlue.batchDeletePartition(any(BatchDeletePartitionRequest.class))).thenThrow(badRequest);
        when(mockGlue.updateTable(any(UpdateTableRequest.class))).thenThrow(badRequest);
        when(mockGlue.createDatabase(any(CreateDatabaseRequest.class))).thenThrow(badRequest);

        Object processor = createProcessor(agent, mockGlue, mockCwlr, mockMetrics, 100);

        CatalogOperation op = buildOperationForType(type);

        invokeExecuteOperation(processor, op);

        // Verify OperationFailure was recorded with the correct type
        verify(mockMetrics).recordOperationFailure(type);
        verify(mockMetrics, never()).recordOperationSuccess(any());
    }

    // ---- Helper to build a CatalogOperation for any OperationType ----

    private CatalogOperation buildOperationForType(CatalogOperation.OperationType type) {
        switch (type) {
            case CREATE_TABLE:
                return new CatalogOperation(type, "testdb", "testtable",
                    buildMinimalTableInput("testtable"));
            case DROP_TABLE:
                return new CatalogOperation(type, "testdb", "testtable", null);
            case ADD_PARTITIONS:
                return new CatalogOperation(type, "testdb", "testtable",
                    Collections.emptyList());
            case DROP_PARTITIONS:
                return new CatalogOperation(type, "testdb", "testtable",
                    Collections.emptyList());
            case UPDATE_TABLE:
                return new CatalogOperation(type, "testdb", "testtable",
                    buildMinimalTableInput("testtable"));
            case CREATE_DATABASE:
                return new CatalogOperation(type, "testdb", null,
                    new DatabaseInput().withName("testdb"));
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }
}
