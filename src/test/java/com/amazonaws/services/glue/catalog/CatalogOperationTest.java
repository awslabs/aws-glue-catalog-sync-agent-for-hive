package com.amazonaws.services.glue.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class CatalogOperationTest {

    @Test
    public void testImmutableFieldsAreSetCorrectly() {
        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.CREATE_TABLE, "mydb", "mytable", "payload_data");

        assertEquals(CatalogOperation.OperationType.CREATE_TABLE, op.getType());
        assertEquals("mydb", op.getDatabaseName());
        assertEquals("mytable", op.getTableName());
        assertEquals("payload_data", op.<String>getPayload());
        assertTrue(op.getTimestamp() > 0);
    }

    @Test
    public void testGetFullTableNameWithTable() {
        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.DROP_TABLE, "mydb", "mytable", null);

        assertEquals("mydb.mytable", op.getFullTableName());
    }

    @Test
    public void testGetFullTableNameWithoutTable() {
        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.CREATE_DATABASE, "mydb", null, null);

        assertEquals("mydb", op.getFullTableName());
    }

    @Test
    public void testAllOperationTypes() {
        CatalogOperation.OperationType[] types = CatalogOperation.OperationType.values();
        assertEquals(6, types.length);
        assertEquals(CatalogOperation.OperationType.CREATE_TABLE, types[0]);
        assertEquals(CatalogOperation.OperationType.DROP_TABLE, types[1]);
        assertEquals(CatalogOperation.OperationType.ADD_PARTITIONS, types[2]);
        assertEquals(CatalogOperation.OperationType.DROP_PARTITIONS, types[3]);
        assertEquals(CatalogOperation.OperationType.UPDATE_TABLE, types[4]);
        assertEquals(CatalogOperation.OperationType.CREATE_DATABASE, types[5]);
    }

    @Test
    public void testNullPayload() {
        CatalogOperation op = new CatalogOperation(
            CatalogOperation.OperationType.DROP_TABLE, "mydb", "mytable", null);

        assertNull(op.<Object>getPayload());
    }

    @Test
    public void testTimestampOrdering() {
        CatalogOperation first = new CatalogOperation(
            CatalogOperation.OperationType.CREATE_TABLE, "db", "t1", null);
        CatalogOperation second = new CatalogOperation(
            CatalogOperation.OperationType.DROP_TABLE, "db", "t1", null);

        assertTrue(second.getTimestamp() >= first.getTimestamp());
    }
}
