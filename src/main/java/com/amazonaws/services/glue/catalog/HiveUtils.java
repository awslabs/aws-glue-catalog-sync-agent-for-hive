package com.amazonaws.services.glue.catalog;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.ql.exec.DDLTask.appendSerdeParams;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

public class HiveUtils {
	private static final Logger LOG = LoggerFactory.getLogger(HiveUtils.class);

	// Copied from Hive's code, it's a private function so had to copy it instead of
	// reusing.
	// License: Apache-2.0
	// File:
	// https://github.com/apache/hive/blob/259db56e359990a1c2830045c423453ed65b76fc/ql/src/java/org/apache/hadoop/hive/ql/exec/DDLTask.java
	protected static String propertiesToString(Map<String, String> props, List<String> exclude) {
		String prop_string = "";
		if (!props.isEmpty()) {
			Map<String, String> properties = new TreeMap<String, String>(props);
			List<String> realProps = new ArrayList<String>();
			for (String key : properties.keySet()) {
				if (properties.get(key) != null && (exclude == null || !exclude.contains(key))) {
					realProps.add("  '" + key + "'='" + HiveStringUtils.escapeHiveCommand(properties.get(key)) + "'");
				}
			}
			prop_string += StringUtils.join(realProps, ", \n");
		}
		return prop_string;
	}

	// Copied from Hive's code, it's a private function so had to copy it instead of
	// reusing.
	// License: Apache-2.0
	// File:
	// https://github.com/apache/hive/blob/259db56e359990a1c2830045c423453ed65b76fc/ql/src/java/org/apache/hadoop/hive/ql/exec/DDLTask.java
	protected static String showCreateTable(org.apache.hadoop.hive.metastore.api.Table msTbl) throws HiveException {
		final String EXTERNAL = "external";
		final String TEMPORARY = "temporary";
		final String LIST_COLUMNS = "columns";
		final String TBL_COMMENT = "tbl_comment";
		final String LIST_PARTITIONS = "partitions";
		final String SORT_BUCKET = "sort_bucket";
		final String SKEWED_INFO = "tbl_skewedinfo";
		final String ROW_FORMAT = "row_format";
		final String TBL_LOCATION = "tbl_location";
		final String TBL_PROPERTIES = "tbl_properties";
		boolean needsLocation = true;
		StringBuilder createTab_str = new StringBuilder();
		String tableName = msTbl.getTableName();
		String dbName = msTbl.getDbName();

		String retVal = null;
		// The code to reuse msTbl as Hive's metadata table is picked from:
		// https://github.com/apache/hive/blob/branch-3.1/ql/src/java/org/apache/hadoop/hive/ql/metadata/Hive.java#L1126

		// For non-views, we need to do some extra fixes
		if (!TableType.VIRTUAL_VIEW.toString().equals(msTbl.getTableType())) {
			// Fix the non-printable chars
			Map<String, String> parameters = msTbl.getSd().getParameters();
			String sf = parameters!=null?parameters.get(SERIALIZATION_FORMAT) : null;
			if (sf != null) {
				char[] b = sf.toCharArray();
				if ((b.length == 1) && (b[0] < 10)) { // ^A, ^B, ^C, ^D, \t
					parameters.put(SERIALIZATION_FORMAT, Integer.toString(b[0]));
				}
			}

			// Use LazySimpleSerDe for MetadataTypedColumnsetSerDe.
			// NOTE: LazySimpleSerDe does not support tables with a single column of
			// col
			// of type "array<string>". This happens when the table is created using
			// an
			// earlier version of Hive.
			if (org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
							.getName().equals(
											msTbl.getSd().getSerdeInfo().getSerializationLib())
							&& msTbl.getSd().getColsSize() > 0
							&& msTbl.getSd().getCols().get(0).getType().indexOf('<') == -1) {
				msTbl.getSd().getSerdeInfo().setSerializationLib(
								org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
			}
		}

		org.apache.hadoop.hive.ql.metadata.Table tbl = new Table(msTbl);
		List<String> duplicateProps = new ArrayList<String>();
		try {
			createTab_str.append("CREATE <" + TEMPORARY + "><" + EXTERNAL + ">TABLE `");
			createTab_str.append(dbName + "." + tableName + "`(\n");
			createTab_str.append("<" + LIST_COLUMNS + ">)\n");
			createTab_str.append("<" + TBL_COMMENT + ">\n");
			createTab_str.append("<" + LIST_PARTITIONS + ">\n");
			createTab_str.append("<" + SORT_BUCKET + ">\n");
			createTab_str.append("<" + SKEWED_INFO + ">\n");
			createTab_str.append("<" + ROW_FORMAT + ">\n");
			if (needsLocation) {
				createTab_str.append("LOCATION\n");
				createTab_str.append("<" + TBL_LOCATION + ">\n");
			}
			createTab_str.append("TBLPROPERTIES (\n");
			createTab_str.append("<" + TBL_PROPERTIES + ">)\n");
			ST createTab_stmt = new ST(createTab_str.toString());

			// For cases where the table is temporary
			String tbl_temp = "";
			if (tbl.isTemporary()) {
				duplicateProps.add("TEMPORARY");
				tbl_temp = "TEMPORARY ";
			}
			// For cases where the table is external
			String tbl_external = "";
			if (tbl.getTableType() == TableType.EXTERNAL_TABLE) {
				duplicateProps.add("EXTERNAL");
				tbl_external = "EXTERNAL ";
			}

			// Columns
			String tbl_columns = "";
			List<FieldSchema> cols = tbl.getCols();
			List<String> columns = new ArrayList<String>();
			for (FieldSchema col : cols) {
				String columnDesc = "  `" + col.getName() + "` " + col.getType();
				if (col.getComment() != null) {
					columnDesc = columnDesc + " COMMENT '" + HiveStringUtils.escapeHiveCommand(col.getComment()) + "'";
				}
				columns.add(columnDesc);
			}
			tbl_columns = StringUtils.join(columns, ", \n");

			// Table comment
			String tbl_comment = "";
			String tabComment = tbl.getProperty("comment");
			if (tabComment != null) {
				duplicateProps.add("comment");
				tbl_comment = "COMMENT '" + HiveStringUtils.escapeHiveCommand(tabComment) + "'";
			}

			// Partitions
			String tbl_partitions = "";
			List<FieldSchema> partKeys = tbl.getPartitionKeys();
			if (partKeys.size() > 0) {
				tbl_partitions += "PARTITIONED BY ( \n";
				List<String> partCols = new ArrayList<String>();
				for (FieldSchema partKey : partKeys) {
					String partColDesc = "  `" + partKey.getName() + "` " + partKey.getType();
					if (partKey.getComment() != null) {
						partColDesc = partColDesc + " COMMENT '"
								+ HiveStringUtils.escapeHiveCommand(partKey.getComment()) + "'";
					}
					partCols.add(partColDesc);
				}
				tbl_partitions += StringUtils.join(partCols, ", \n");
				tbl_partitions += ")";
			}

			// Clusters (Buckets)
			String tbl_sort_bucket = "";
			List<String> buckCols = tbl.getBucketCols();
			if (buckCols.size() > 0) {
				duplicateProps.add("SORTBUCKETCOLSPREFIX");
				tbl_sort_bucket += "CLUSTERED BY ( \n  ";
				tbl_sort_bucket += StringUtils.join(buckCols, ", \n  ");
				tbl_sort_bucket += ") \n";
				List<Order> sortCols = tbl.getSortCols();
				if (sortCols.size() > 0) {
					tbl_sort_bucket += "SORTED BY ( \n";
					// Order
					List<String> sortKeys = new ArrayList<String>();
					for (Order sortCol : sortCols) {
						String sortKeyDesc = "  " + sortCol.getCol() + " ";
						if (sortCol.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC) {
							sortKeyDesc = sortKeyDesc + "ASC";
						} else if (sortCol.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_DESC) {
							sortKeyDesc = sortKeyDesc + "DESC";
						}
						sortKeys.add(sortKeyDesc);
					}
					tbl_sort_bucket += StringUtils.join(sortKeys, ", \n");
					tbl_sort_bucket += ") \n";
				}
				tbl_sort_bucket += "INTO " + tbl.getNumBuckets() + " BUCKETS";
			}

			// Skewed Info
			StringBuilder tbl_skewedinfo = new StringBuilder();
			SkewedInfo skewedInfo = tbl.getSkewedInfo();
			if (skewedInfo != null && !skewedInfo.getSkewedColNames().isEmpty()) {
				tbl_skewedinfo.append("SKEWED BY (" + StringUtils.join(skewedInfo.getSkewedColNames(), ",") + ")\n");
				tbl_skewedinfo.append("  ON (");
				List<String> colValueList = new ArrayList<String>();
				for (List<String> colValues : skewedInfo.getSkewedColValues()) {
					colValueList.add("('" + StringUtils.join(colValues, "','") + "')");
				}
				tbl_skewedinfo.append(StringUtils.join(colValueList, ",") + ")");
				if (tbl.isStoredAsSubDirectories()) {
					tbl_skewedinfo.append("\n  STORED AS DIRECTORIES");
				}
			}

			// Row format (SerDe)
			StringBuilder tbl_row_format = new StringBuilder();
			StorageDescriptor sd = tbl.getTTable().getSd();
			SerDeInfo serdeInfo = sd.getSerdeInfo();
			Map<String, String> serdeParams = serdeInfo.getParameters();
			tbl_row_format.append("ROW FORMAT SERDE \n");
			tbl_row_format.append("  '" + HiveStringUtils.escapeHiveCommand(serdeInfo.getSerializationLib()) + "' \n");
			if (tbl.getStorageHandler() == null) {
				// If serialization.format property has the default value, it will not to be
				// included in
				// SERDE properties
				if (Warehouse.DEFAULT_SERIALIZATION_FORMAT
						.equals(serdeParams.get(serdeConstants.SERIALIZATION_FORMAT))) {
					serdeParams.remove(serdeConstants.SERIALIZATION_FORMAT);
				}
				if (!serdeParams.isEmpty()) {
					appendSerdeParams(tbl_row_format, serdeParams).append(" \n");
				}
				tbl_row_format.append("STORED AS INPUTFORMAT \n  '"
						+ HiveStringUtils.escapeHiveCommand(sd.getInputFormat()) + "' \n");
				tbl_row_format
						.append("OUTPUTFORMAT \n  '" + HiveStringUtils.escapeHiveCommand(sd.getOutputFormat()) + "'");
			} else {
				duplicateProps.add(META_TABLE_STORAGE);
				tbl_row_format.append("STORED BY \n  '"
						+ HiveStringUtils.escapeHiveCommand(tbl.getParameters().get(META_TABLE_STORAGE)) + "' \n");
				// SerDe Properties
				if (!serdeParams.isEmpty()) {
					appendSerdeParams(tbl_row_format, serdeInfo.getParameters());
				}
			}

			String tbl_location = "  '" + HiveStringUtils.escapeHiveCommand(sd.getLocation()) + "'";

			// Replace s3a/s3n with s3
			tbl_location = tbl_location.replaceFirst("s3[a,n]://", "s3://");

			// Table properties
			duplicateProps.addAll(Arrays.asList(StatsSetupConst.TABLE_PARAMS_STATS_KEYS));
			String tbl_properties = propertiesToString(tbl.getParameters(), duplicateProps);

			createTab_stmt.add(TEMPORARY, tbl_temp);
			createTab_stmt.add(EXTERNAL, tbl_external);
			createTab_stmt.add(LIST_COLUMNS, tbl_columns);
			createTab_stmt.add(TBL_COMMENT, tbl_comment);
			createTab_stmt.add(LIST_PARTITIONS, tbl_partitions);
			createTab_stmt.add(SORT_BUCKET, tbl_sort_bucket);
			createTab_stmt.add(SKEWED_INFO, tbl_skewedinfo);
			createTab_stmt.add(ROW_FORMAT, tbl_row_format);
			// Table location should not be printed with hbase backed tables
			if (needsLocation) {
				createTab_stmt.add(TBL_LOCATION, tbl_location);
			}
			createTab_stmt.add(TBL_PROPERTIES, tbl_properties);
			retVal = createTab_stmt.render();

		} catch (Exception e) {
			LOG.error("show create table: ", e);
			retVal = null;
		}

		return retVal;
	}

}
