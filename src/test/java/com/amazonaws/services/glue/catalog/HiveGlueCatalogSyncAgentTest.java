package com.amazonaws.services.glue.catalog;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
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

}
