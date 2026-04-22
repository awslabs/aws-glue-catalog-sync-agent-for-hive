package com.amazonaws.services.glue.catalog;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

/**
 * Property-based tests for CloudWatch Log message content.
 *
 * Feature: gdc-direct-api-sync, Property 12: CloudWatch Log Messages Contain Required Fields
 *
 * For any CatalogOperation execution (success or failure), the message sent to
 * CloudWatch Logs SHALL contain the operation type, database name, and table name.
 * For failed operations, the message SHALL additionally contain the error details.
 *
 * Validates: Requirements 14.1, 14.2
 */
public class CWLPropertyTest {

    // ---- Generators ----

    @Provide
    Arbitrary<String> dbNames() {
        return Arbitraries.strings().withCharRange('a', 'z').ofMinLength(1).ofMaxLength(12);
    }

    @Provide
    Arbitrary<String> tableNames() {
        return Arbitraries.strings().withCharRange('a', 'z').ofMinLength(1).ofMaxLength(12);
    }

    @Provide
    Arbitrary<CatalogOperation.OperationType> tableOperationTypes() {
        return Arbitraries.of(
            CatalogOperation.OperationType.CREATE_TABLE,
            CatalogOperation.OperationType.DROP_TABLE,
            CatalogOperation.OperationType.ADD_PARTITIONS,
            CatalogOperation.OperationType.DROP_PARTITIONS,
            CatalogOperation.OperationType.UPDATE_TABLE
        );
    }

    @Provide
    Arbitrary<CatalogOperation.OperationType> allOperationTypes() {
        return Arbitraries.of(CatalogOperation.OperationType.values());
    }

    @Provide
    Arbitrary<String> errorMessages() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(30);
    }

    // ---- Helpers ----

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

    private CatalogOperation buildOperation(CatalogOperation.OperationType type, String dbName, String tableName) {
        Object payload;
        switch (type) {
            case CREATE_TABLE:
            case UPDATE_TABLE:
                payload = buildMinimalTableInput(tableName);
                break;
            case ADD_PARTITIONS:
                PartitionInput pi = new PartitionInput()
                    .withValues(Arrays.asList("2024", "01"))
                    .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("s3://bucket/part")
                        .withInputFormat("org.apache.hadoop.mapred.TextInputFormat")
                        .withOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                        .withColumns(Collections.singletonList(new Column().withName("col1").withType("string")))
                        .withSerdeInfo(new SerDeInfo()
                            .withSerializationLibrary("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")));
                payload = Collections.singletonList(pi);
                break;
            case DROP_PARTITIONS:
                PartitionValueList pvl = new PartitionValueList().withValues(Arrays.asList("2024", "01"));
                payload = Collections.singletonList(pvl);
                break;
            case DROP_TABLE:
                payload = null;
                break;
            case CREATE_DATABASE:
                payload = new DatabaseInput().withName(dbName);
                break;
            default:
                payload = null;
        }
        String tblName = (type == CatalogOperation.OperationType.CREATE_DATABASE) ? null : tableName;
        return new CatalogOperation(type, dbName, tblName, payload);
    }

    /**
     * Creates a GlueCatalogQueueProcessor via reflection (non-static inner class).
     */
    private Object createProcessor(AWSGlue mockGlue, CloudWatchLogsReporter mockCwlr) throws Exception {
        HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();
        Class<?> processorClass = Class.forName(
            "com.amazonaws.services.glue.catalog.HiveGlueCatalogSyncAgent$GlueCatalogQueueProcessor");
        java.lang.reflect.Constructor<?> ctor = processorClass.getDeclaredConstructor(
            HiveGlueCatalogSyncAgent.class,
            AWSGlue.class,
            CloudWatchLogsReporter.class,
            MetricsCollector.class,
            String.class,
            boolean.class,
            boolean.class,
            int.class
        );
        ctor.setAccessible(true);
        return ctor.newInstance(agent, mockGlue, mockCwlr, new NoOpMetricsCollector(), null, false, false, 100);
    }

    /**
     * Invokes executeOperation via reflection.
     */
    private void invokeExecuteOperation(Object processor, CatalogOperation op) throws Exception {
        Method method = processor.getClass().getDeclaredMethod("executeOperation", CatalogOperation.class);
        method.setAccessible(true);
        method.invoke(processor, op);
    }

    // ========================================================================
    // Property 12: CloudWatch Log Messages Contain Required Fields
    // Feature: gdc-direct-api-sync, Property 12: CloudWatch Log Messages Contain Required Fields
    // ========================================================================

    /**
     * Validates: Requirements 14.1, 14.2
     *
     * For any successful CatalogOperation with a table (non-CREATE_DATABASE),
     * the CWL message contains the operation type name, database name, and table name.
     */
    @Property(tries = 100)
    void successfulTableOperationCWLContainsRequiredFields(
            @ForAll("tableOperationTypes") CatalogOperation.OperationType opType,
            @ForAll("dbNames") String dbName,
            @ForAll("tableNames") String tableName) throws Exception {

        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);

        Object processor = createProcessor(mockGlue, mockCwlr);
        CatalogOperation op = buildOperation(opType, dbName, tableName);

        invokeExecuteOperation(processor, op);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());

        List<String> messages = captor.getAllValues();
        // At least one message should contain the operation type, db name, and table name
        boolean found = false;
        for (String msg : messages) {
            if (msg.contains(opType.name()) && msg.contains(dbName) && msg.contains(tableName)) {
                found = true;
                break;
            }
        }
        assert found : "CWL message should contain operation type '" + opType.name()
            + "', db '" + dbName + "', table '" + tableName
            + "' but messages were: " + messages;
    }

    /**
     * Validates: Requirements 14.1
     *
     * For a successful CREATE_DATABASE operation, the CWL message contains
     * the operation type name and database name.
     */
    @Property(tries = 100)
    void successfulCreateDatabaseCWLContainsRequiredFields(
            @ForAll("dbNames") String dbName) throws Exception {

        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);

        Object processor = createProcessor(mockGlue, mockCwlr);
        CatalogOperation op = buildOperation(CatalogOperation.OperationType.CREATE_DATABASE, dbName, null);

        invokeExecuteOperation(processor, op);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());

        List<String> messages = captor.getAllValues();
        boolean found = false;
        for (String msg : messages) {
            if (msg.contains("CREATE_DATABASE") && msg.contains(dbName)) {
                found = true;
                break;
            }
        }
        assert found : "CWL message should contain 'CREATE_DATABASE' and db '" + dbName
            + "' but messages were: " + messages;
    }

    /**
     * Validates: Requirements 14.2
     *
     * For any failed CatalogOperation (non-transient error), the CWL message
     * contains the operation type, database name, table name, AND error details.
     */
    @Property(tries = 100)
    void failedTableOperationCWLContainsErrorDetails(
            @ForAll("tableOperationTypes") CatalogOperation.OperationType opType,
            @ForAll("dbNames") String dbName,
            @ForAll("tableNames") String tableName,
            @ForAll("errorMessages") String errorMsg) throws Exception {

        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);

        // Configure mock to throw a non-transient error (status 400)
        AmazonServiceException ase = new AmazonServiceException(errorMsg);
        ase.setStatusCode(400);

        // Set up the mock to throw for any GDC API call
        when(mockGlue.createTable(any(CreateTableRequest.class))).thenThrow(ase);
        when(mockGlue.deleteTable(any(DeleteTableRequest.class))).thenThrow(ase);
        when(mockGlue.batchCreatePartition(any(BatchCreatePartitionRequest.class))).thenThrow(ase);
        when(mockGlue.batchDeletePartition(any(BatchDeletePartitionRequest.class))).thenThrow(ase);
        when(mockGlue.updateTable(any(UpdateTableRequest.class))).thenThrow(ase);

        Object processor = createProcessor(mockGlue, mockCwlr);
        CatalogOperation op = buildOperation(opType, dbName, tableName);

        invokeExecuteOperation(processor, op);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());

        List<String> messages = captor.getAllValues();
        // Find the error message that contains all required fields
        boolean foundError = false;
        for (String msg : messages) {
            if (msg.contains(opType.name())
                    && msg.contains(dbName)
                    && msg.contains(tableName)
                    && msg.contains(errorMsg)) {
                foundError = true;
                break;
            }
        }
        assert foundError : "Error CWL message should contain operation type '" + opType.name()
            + "', db '" + dbName + "', table '" + tableName
            + "', error '" + errorMsg
            + "' but messages were: " + messages;
    }

    /**
     * Validates: Requirements 14.2
     *
     * For a failed CREATE_DATABASE operation (non-transient error), the CWL message
     * contains the operation type, database name, AND error details.
     */
    @Property(tries = 100)
    void failedCreateDatabaseCWLContainsErrorDetails(
            @ForAll("dbNames") String dbName,
            @ForAll("errorMessages") String errorMsg) throws Exception {

        AWSGlue mockGlue = mock(AWSGlue.class);
        CloudWatchLogsReporter mockCwlr = mock(CloudWatchLogsReporter.class);

        AmazonServiceException ase = new AmazonServiceException(errorMsg);
        ase.setStatusCode(400);
        when(mockGlue.createDatabase(any(CreateDatabaseRequest.class))).thenThrow(ase);

        Object processor = createProcessor(mockGlue, mockCwlr);
        CatalogOperation op = buildOperation(CatalogOperation.OperationType.CREATE_DATABASE, dbName, null);

        invokeExecuteOperation(processor, op);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockCwlr, atLeastOnce()).sendToCWL(captor.capture());

        List<String> messages = captor.getAllValues();
        boolean foundError = false;
        for (String msg : messages) {
            if (msg.contains("CREATE_DATABASE") && msg.contains(dbName) && msg.contains(errorMsg)) {
                foundError = true;
                break;
            }
        }
        assert foundError : "Error CWL message should contain 'CREATE_DATABASE', db '" + dbName
            + "', error '" + errorMsg + "' but messages were: " + messages;
    }
}
