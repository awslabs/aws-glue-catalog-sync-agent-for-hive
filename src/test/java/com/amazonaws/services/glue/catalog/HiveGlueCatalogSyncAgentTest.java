package com.amazonaws.services.glue.catalog;

import static com.amazonaws.services.glue.catalog.HiveGlueCatalogSyncAgent.alterTableRequiresDropTable;
import static com.amazonaws.services.glue.catalog.HiveGlueCatalogSyncAgent.createAthenaAlterTableAddColumnsStatement;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class HiveGlueCatalogSyncAgentTest {
	HiveGlueCatalogSyncAgent agent;

	public HiveGlueCatalogSyncAgentTest() throws Exception {
		super();
		this.agent = new HiveGlueCatalogSyncAgent();
	}

	@Test
	public void testPartitionSpec() {
		Table table = new Table();
		table.setDbName("test");
		table.setTableName("test_table");

		List<FieldSchema> partitionKeys = new ArrayList<FieldSchema>() {
			{
				add(new FieldSchema("column_1", "string", "first column"));
				add(new FieldSchema("column_2", "date", "second column"));
				add(new FieldSchema("column_3", "int", "third column"));
			}
		};

		table.setPartitionKeys(partitionKeys);

		Partition partition = new Partition();
		String a = "string_value_1";
		String b = "1970-01-01";
		String c = "1";
		
		List<String> valueList = new ArrayList<String>() {
			{

				add(a);
				add(b);
				add(c);
			}
		};
		partition.setValues(valueList);
		String response = this.agent.getPartitionSpec(table, partition);

		// check that first 2 columns are quoted, last one isn't
		assertEquals(String.format("column_1='%s',column_2='%s',column_3=%s",a,b,c), response);
	}

	private static final Table getTable(final List<FieldSchema> columns) {
		final Table table = new Table();
		table.setDbName("test");
		table.setTableName("test_table");

		final StorageDescriptor sd = new StorageDescriptor();
		sd.setCols(columns);
		table.setSd(sd);

		return table;
	}

	@Test
	public void testCreateAthenaAlterTableStatementAddSingleColumn() {
		final Table oldTable = getTable(
			asList(
				new FieldSchema("column_1", "string", "first column"),
				new FieldSchema("column_2", "timestamp", "second column")
			)
		);
		final Table newTable = getTable(
			asList(
				new FieldSchema("column_1", "string", "first column"),
				new FieldSchema("column_2", "timestamp", "second column"),
				new FieldSchema("column_3", "int", "third column")
			)
		);

		assertEquals(
			"alter table test.test_table add columns (column_3 int)",
			createAthenaAlterTableAddColumnsStatement(oldTable, newTable)
		);
	}

	@Test
	public void testCreateAthenaAlterTableStatementAddMultipleColumns() {
		final Table oldTable = getTable(
			asList(
				new FieldSchema("column_1", "string", "first column"),
				new FieldSchema("column_2", "timestamp", "second column")
			)
		);
		final Table newTable = getTable(
			asList(
				new FieldSchema("column_1", "string", "first column"),
				new FieldSchema("column_2", "timestamp", "second column"),
				new FieldSchema("column_3", "int", "third column"),
        new FieldSchema("column_4", "bigint", "fourth column")
			)
		);

		assertEquals(
			"alter table test.test_table add columns (column_3 int, column_4 bigint)",
			createAthenaAlterTableAddColumnsStatement(oldTable, newTable)
		);
	}

	@Test
	public void testAlterTableRequiresDropTable() {
		final Table oldTable = getTable(
			asList(
				new FieldSchema("column_1", "string", "first column")
			)
		);
		final Table newTableChangedDataType = getTable(
			asList(
				new FieldSchema("column_1", "int", "first column")
			)
		);
		final Table newTableRemovedColumn = getTable(
			asList(
				new FieldSchema("column_2", "string", "second column")
			)
		);
		final Table newTableAddedNewColumn = getTable(
			asList(
				new FieldSchema("column_1", "string", "first column"),
				new FieldSchema("column_2", "int", "second column")
			)
		);
		final Table newTableChangedComment = getTable(
			asList(
				new FieldSchema("column_1", "string", "some first column")
			)
		);

		assertTrue(
			alterTableRequiresDropTable(oldTable, newTableChangedDataType)
		);

		assertTrue(
			alterTableRequiresDropTable(oldTable, newTableRemovedColumn)
		);

		assertFalse(
			alterTableRequiresDropTable(oldTable, newTableAddedNewColumn)
		);

		assertFalse(
			alterTableRequiresDropTable(oldTable, newTableChangedComment)
		);
	}
}
