package com.amazonaws.services.glue.catalog;

/**
 * Immutable value object representing a single catalog sync action.
 * Replaces DDL strings in the operation queue with structured operation objects
 * that can be processed directly by the GDC API queue processor.
 */
public class CatalogOperation {

    public enum OperationType {
        CREATE_TABLE, DROP_TABLE, ADD_PARTITIONS, DROP_PARTITIONS, UPDATE_TABLE, CREATE_DATABASE
    }

    private final OperationType type;
    private final String databaseName;
    private final String tableName;       // null for CREATE_DATABASE
    private final Object payload;         // type-specific payload
    private final long timestamp;         // for ordering within batch
    private final long createdAtMillis;   // wall-clock time for sync lag calculation

    public CatalogOperation(OperationType type, String databaseName, String tableName, Object payload) {
        this.type = type;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.payload = payload;
        this.timestamp = System.nanoTime();
        this.createdAtMillis = System.currentTimeMillis();
    }

    public OperationType getType() { return type; }
    public String getDatabaseName() { return databaseName; }
    public String getTableName() { return tableName; }

    @SuppressWarnings("unchecked")
    public <T> T getPayload() { return (T) payload; }

    public long getTimestamp() { return timestamp; }
    public long getCreatedAtMillis() { return createdAtMillis; }

    public String getFullTableName() {
        return tableName != null ? databaseName + "." + tableName : databaseName;
    }
}
