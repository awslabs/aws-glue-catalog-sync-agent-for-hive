package com.amazonaws.services.glue.catalog;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for GlueCatalogQueueProcessor error handling scenarios.
 *
 * Validates: Requirements 4.4, 4.5, 5.4, 6.5, 7.5, 11.2, 11.3
 */
public class GlueCatalogQueueProcessorTest {

    // ---- Reflection helpers ----

    private Object createProcessor(HiveGlueCatalogSyncAgent agent, AWSGlue mockGlue,
            CloudWatchLogsReporter mockCwlr, boolean dropTableIfExists, boolean createMissingDB) throws Exception {
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
            new NoOpMetricsCollector(),
            null, dropTableIfExists, createMissingDB,
            100);
    }

    private void invokeExecuteOperation(Object processor, CatalogOperation op) throws Exception {
        Method method = processor.getClass().getDeclaredMethod("executeOperation", CatalogOperation.class);
        method.setAccessible(true);
        method.invoke(processor, op);
    }

    @SuppressWarnings("unchecked")
    private Set<String> getDisallowedTables(HiveGlueCatalogSyncAgent agent) throws Exception {
        Field field = HiveGlueCatalogSyncAgent.class.getDeclaredField("disallowedTables");
        field.setAccessible(true);
        return (Set<String>) field.get(agent);
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

    // ---- Test 1: AlreadyExistsException + dropTableIfExists=true ----

    /**
     * Validates: Requirement 4.4
     * IF createTable returns AlreadyExistsException and dropTableIfExists is enabled,
     * THEN the processor drops the table and retries createTable.
     */
    @Test
    public void testAlreadyExistsWithDropTableIfExistsTrue() throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        Object processor = createProcessor(agent, mockGlue, mockCwlr, true, false);

        // First createTable call throws AlreadyExistsException; second succeeds
        when(mockGlue.createTable(any(CreateTableRequest.class)))
            .thenThrow(new AlreadyExistsException("Table already exists"))
            .thenReturn(null);

        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.CREATE_TABLE, "mydb", "mytable",
            buildMinimalTableInput("mytable"));

        invokeExecuteOperation(processor, op);

        // deleteTable should have been called to drop the existing table
        verify(mockGlue).deleteTable(any(DeleteTableRequest.class));
        // createTable should have been called twice (initial + retry after drop)
        verify(mockGlue, times(2)).createTable(any(CreateTableRequest.class));
        // CWL should report success
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());
        boolean hasSuccess = captor.getAllValues().stream()
            .anyMatch(msg -> msg.contains("SUCCESS") && msg.contains("mydb.mytable"));
        assertTrue("CWL should contain a success message for drop+retry", hasSuccess);
    }

    // ---- Test 2: AlreadyExistsException + dropTableIfExists=false ----

    /**
     * Validates: Requirement 4.5
     * IF createTable returns AlreadyExistsException and dropTableIfExists is disabled,
     * THEN the processor blacklists the table and logs to CWL.
     */
    @Test
    public void testAlreadyExistsWithDropTableIfExistsFalse() throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        Object processor = createProcessor(agent, mockGlue, mockCwlr, false, false);

        when(mockGlue.createTable(any(CreateTableRequest.class)))
            .thenThrow(new AlreadyExistsException("Table already exists"));

        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.CREATE_TABLE, "mydb", "mytable",
            buildMinimalTableInput("mytable"));

        invokeExecuteOperation(processor, op);

        // deleteTable should NOT have been called
        verify(mockGlue, never()).deleteTable(any(DeleteTableRequest.class));
        // CWL should report blacklisting
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());
        boolean hasBlacklist = captor.getAllValues().stream()
            .anyMatch(msg -> msg.contains("DISALLOWED") && msg.contains("mydb.mytable"));
        assertTrue("CWL should contain a blacklist message", hasBlacklist);
        // Verify the table is in the blacklist
        Set<String> disallowed = getDisallowedTables(agent);
        assertTrue("Table should be disallowed", disallowed.contains("mydb.mytable"));
    }

    // ---- Test 3: EntityNotFoundException on createTable + createMissingDB=true ----

    /**
     * Validates: Requirement 4.4 (EntityNotFoundException variant)
     * IF createTable returns EntityNotFoundException with "database" in message
     * and createMissingDB is enabled, THEN the processor creates the database and retries.
     */
    @Test
    public void testEntityNotFoundOnCreateTableWithCreateMissingDB() throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        Object processor = createProcessor(agent, mockGlue, mockCwlr, false, true);

        // First createTable throws EntityNotFoundException (database not found); second succeeds
        EntityNotFoundException enfe = new EntityNotFoundException("database not found");
        when(mockGlue.createTable(any(CreateTableRequest.class)))
            .thenThrow(enfe)
            .thenReturn(null);

        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.CREATE_TABLE, "mydb", "mytable",
            buildMinimalTableInput("mytable"));

        invokeExecuteOperation(processor, op);

        // createDatabase should have been called
        verify(mockGlue).createDatabase(any(CreateDatabaseRequest.class));
        // createTable should have been called twice (initial + retry after createDatabase)
        verify(mockGlue, times(2)).createTable(any(CreateTableRequest.class));
        // CWL should report success
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());
        boolean hasSuccess = captor.getAllValues().stream()
            .anyMatch(msg -> msg.contains("SUCCESS") && msg.contains("mydb.mytable"));
        assertTrue("CWL should contain a success message after createDatabase+retry", hasSuccess);
    }

    // ---- Test 4: EntityNotFoundException on deleteTable ----

    /**
     * Validates: Requirement 5.4
     * IF deleteTable returns EntityNotFoundException, THEN the processor logs
     * an info message and skips without error.
     */
    @Test
    public void testEntityNotFoundOnDeleteTable() throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        Object processor = createProcessor(agent, mockGlue, mockCwlr, false, false);

        when(mockGlue.deleteTable(any(DeleteTableRequest.class)))
            .thenThrow(new EntityNotFoundException("Table not found"));

        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.DROP_TABLE, "mydb", "mytable", null);

        // Should not throw
        invokeExecuteOperation(processor, op);

        // CWL should report info (not error)
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());
        boolean hasInfo = captor.getAllValues().stream()
            .anyMatch(msg -> msg.contains("INFO") && msg.contains("mydb.mytable"));
        assertTrue("CWL should contain an info message for table not found", hasInfo);
        boolean hasError = captor.getAllValues().stream()
            .anyMatch(msg -> msg.startsWith("ERROR"));
        assertFalse("CWL should NOT contain an error message", hasError);
    }

    // ---- Test 5: Transient error is surfaced after SDK retries are exhausted ----

    /**
     * Validates: Requirement 11.2
     * Transient error retries (429, 500, 503) are now handled by the SDK's built-in
     * retry policy. If the SDK exhausts its retries, the exception propagates to the
     * processor which logs the failure.
     */
    @Test
    public void testTransientErrorSurfacedAfterSdkRetries() throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        Object processor = createProcessor(agent, mockGlue, mockCwlr, false, false);

        // Simulate SDK having exhausted its retries and throwing the exception
        AmazonServiceException throttle = new AmazonServiceException("Rate exceeded");
        throttle.setStatusCode(429);
        when(mockGlue.createTable(any(CreateTableRequest.class)))
            .thenThrow(throttle);

        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.CREATE_TABLE, "mydb", "mytable",
            buildMinimalTableInput("mytable"));

        invokeExecuteOperation(processor, op);

        // createTable should have been called exactly once (SDK handles retries internally)
        verify(mockGlue, times(1)).createTable(any(CreateTableRequest.class));
        // CWL should report error
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());
        boolean hasError = captor.getAllValues().stream()
            .anyMatch(msg -> msg.contains("ERROR") && msg.contains("mydb.mytable"));
        assertTrue("CWL should contain an error message after SDK retries exhausted", hasError);
    }

    // ---- Test 6: Non-transient error ----

    /**
     * Validates: Requirement 11.3
     * IF a GDC API call fails with a non-transient error (400), THEN the processor
     * logs the error and moves to the next operation.
     */
    @Test
    public void testNonTransientError() throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        Object processor = createProcessor(agent, mockGlue, mockCwlr, false, false);

        AmazonServiceException badRequest = new AmazonServiceException("Invalid input");
        badRequest.setStatusCode(400);
        when(mockGlue.createTable(any(CreateTableRequest.class)))
            .thenThrow(badRequest);

        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.CREATE_TABLE, "mydb", "mytable",
            buildMinimalTableInput("mytable"));

        invokeExecuteOperation(processor, op);

        // createTable should have been called exactly once (no retry for non-transient)
        verify(mockGlue, times(1)).createTable(any(CreateTableRequest.class));
        // CWL should report error
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());
        boolean hasError = captor.getAllValues().stream()
            .anyMatch(msg -> msg.contains("ERROR") && msg.contains("mydb.mytable"));
        assertTrue("CWL should contain an error message", hasError);
    }

    // ---- Test 7: EntityNotFoundException on batchCreatePartition ----

    /**
     * Validates: Requirement 6.5
     * IF batchCreatePartition returns EntityNotFoundException, THEN the processor
     * logs an info message and skips.
     */
    @Test
    public void testEntityNotFoundOnBatchCreatePartition() throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        Object processor = createProcessor(agent, mockGlue, mockCwlr, false, false);

        when(mockGlue.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
            .thenThrow(new EntityNotFoundException("Table not found"));

        PartitionInput pi = new PartitionInput()
            .withValues(Arrays.asList("2024", "01"))
            .withStorageDescriptor(new StorageDescriptor()
                .withLocation("s3://bucket/part")
                .withInputFormat("org.apache.hadoop.mapred.TextInputFormat")
                .withOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .withColumns(Collections.singletonList(new Column().withName("col1").withType("string")))
                .withSerdeInfo(new SerDeInfo()
                    .withSerializationLibrary("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")));

        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.ADD_PARTITIONS, "mydb", "mytable",
            Collections.singletonList(pi));

        // Should not throw
        invokeExecuteOperation(processor, op);

        // CWL should report info (not error)
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());
        boolean hasInfo = captor.getAllValues().stream()
            .anyMatch(msg -> msg.contains("INFO") && msg.contains("mydb.mytable"));
        assertTrue("CWL should contain an info message for table not found", hasInfo);
        boolean hasError = captor.getAllValues().stream()
            .anyMatch(msg -> msg.startsWith("ERROR"));
        assertFalse("CWL should NOT contain an error message", hasError);
    }

    // ---- Test 8: EntityNotFoundException on batchDeletePartition ----

    /**
     * Validates: Requirement 7.5
     * IF batchDeletePartition returns EntityNotFoundException, THEN the processor
     * logs an info message and skips.
     */
    @Test
    public void testEntityNotFoundOnBatchDeletePartition() throws Exception {
        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();

        Object processor = createProcessor(agent, mockGlue, mockCwlr, false, false);

        when(mockGlue.batchDeletePartition(any(BatchDeletePartitionRequest.class)))
            .thenThrow(new EntityNotFoundException("Table not found"));

        PartitionValueList pvl = new PartitionValueList()
            .withValues(Arrays.asList("2024", "01"));

        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.DROP_PARTITIONS, "mydb", "mytable",
            Collections.singletonList(pvl));

        // Should not throw
        invokeExecuteOperation(processor, op);

        // CWL should report info (not error)
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());
        boolean hasInfo = captor.getAllValues().stream()
            .anyMatch(msg -> msg.contains("INFO") && msg.contains("mydb.mytable"));
        assertTrue("CWL should contain an info message for table not found", hasInfo);
        boolean hasError = captor.getAllValues().stream()
            .anyMatch(msg -> msg.startsWith("ERROR"));
        assertFalse("CWL should NOT contain an error message", hasError);
    }
}
