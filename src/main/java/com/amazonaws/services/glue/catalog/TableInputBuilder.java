package com.amazonaws.services.glue.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that converts Hive Table metadata to AWS Glue TableInput objects.
 * Converts Hive Table metadata into structured Glue API objects.
 */
public class TableInputBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(TableInputBuilder.class);

    /** Hive stats keys to exclude from table parameters when syncStats is false. */
    private static final Set<String> HIVE_STATS_KEYS = new HashSet<>(Arrays.asList(
        "numRows",
        "rawDataSize",
        "totalSize",
        "COLUMN_STATS_ACCURATE",
        "numFiles",
        "transient_lastDdlTime"
    ));

    private TableInputBuilder() {
        // Utility class — not instantiable
    }

    /**
     * Converts a Hive Table to a Glue TableInput.
     *
     * @param hiveTable the Hive Table metadata
     * @param syncStats if false, Hive statistics keys are excluded from parameters
     * @return a Glue TableInput ready for createTable/updateTable API calls
     */
    public static TableInput buildTableInput(Table hiveTable, boolean syncStats) {
        TableInput tableInput = new TableInput()
            .withName(hiveTable.getTableName())
            .withTableType(hiveTable.getTableType())
            .withStorageDescriptor(buildStorageDescriptor(hiveTable.getSd()));

        // Map partition keys
        if (hiveTable.getPartitionKeys() != null && !hiveTable.getPartitionKeys().isEmpty()) {
            tableInput.withPartitionKeys(buildColumns(hiveTable.getPartitionKeys()));
        }

        // Map parameters, filtering stats keys if syncStats is false
        if (hiveTable.getParameters() != null) {
            Map<String, String> params = new HashMap<>(hiveTable.getParameters());
            if (!syncStats) {
                for (String statsKey : HIVE_STATS_KEYS) {
                    params.remove(statsKey);
                }
            }
            tableInput.withParameters(params);
        }

        return tableInput;
    }

    /**
     * Maps a Hive StorageDescriptor to a Glue StorageDescriptor.
     *
     * @param hiveSd the Hive StorageDescriptor
     * @return a Glue StorageDescriptor
     */
    public static StorageDescriptor buildStorageDescriptor(
            org.apache.hadoop.hive.metastore.api.StorageDescriptor hiveSd) {
        StorageDescriptor glueSd = new StorageDescriptor()
            .withInputFormat(hiveSd.getInputFormat())
            .withOutputFormat(hiveSd.getOutputFormat())
            .withLocation(translateLocation(hiveSd.getLocation()))
            .withNumberOfBuckets(hiveSd.getNumBuckets());

        // Map columns
        if (hiveSd.getCols() != null) {
            glueSd.withColumns(buildColumns(hiveSd.getCols()));
        }

        // Map SerDe info
        if (hiveSd.getSerdeInfo() != null) {
            SerDeInfo glueSerDeInfo = new SerDeInfo()
                .withSerializationLibrary(hiveSd.getSerdeInfo().getSerializationLib());
            if (hiveSd.getSerdeInfo().getParameters() != null) {
                glueSerDeInfo.withParameters(new HashMap<>(hiveSd.getSerdeInfo().getParameters()));
            }
            glueSd.withSerdeInfo(glueSerDeInfo);
        }

        // Map bucket columns
        if (hiveSd.getBucketCols() != null && !hiveSd.getBucketCols().isEmpty()) {
            glueSd.withBucketColumns(new ArrayList<>(hiveSd.getBucketCols()));
        }

        // Map sort columns
        if (hiveSd.getSortCols() != null && !hiveSd.getSortCols().isEmpty()) {
            List<Order> glueSortCols = hiveSd.getSortCols().stream()
                .map(hiveOrder -> new Order()
                    .withColumn(hiveOrder.getCol())
                    .withSortOrder(hiveOrder.getOrder()))
                .collect(Collectors.toList());
            glueSd.withSortColumns(glueSortCols);
        }

        // Drop skewed info with a warning
        if (hiveSd.getSkewedInfo() != null) {
            SkewedInfo skewedInfo = hiveSd.getSkewedInfo();
            if (skewedInfo.getSkewedColNames() != null && !skewedInfo.getSkewedColNames().isEmpty()) {
                LOG.warn("Skewed table info is not supported by AWS Glue Data Catalog and will be dropped. "
                    + "Skewed columns: {}", skewedInfo.getSkewedColNames());
            }
        }

        return glueSd;
    }

    /**
     * Maps a list of Hive FieldSchema to a list of Glue Column objects.
     *
     * @param fields the Hive field schema list
     * @return a list of Glue Column objects
     */
    public static List<Column> buildColumns(List<FieldSchema> fields) {
        if (fields == null) {
            return new ArrayList<>();
        }
        return fields.stream()
            .map(field -> {
                Column col = new Column()
                    .withName(field.getName())
                    .withType(field.getType());
                if (field.getComment() != null) {
                    col.withComment(field.getComment());
                }
                return col;
            })
            .collect(Collectors.toList());
    }

    /**
     * Converts a Hive Partition to a Glue PartitionInput.
     *
     * @param partition the Hive Partition
     * @return a Glue PartitionInput
     */
    public static PartitionInput buildPartitionInput(Partition partition) {
        PartitionInput partitionInput = new PartitionInput();

        if (partition.getValues() != null) {
            partitionInput.withValues(new ArrayList<>(partition.getValues()));
        }

        if (partition.getSd() != null) {
            partitionInput.withStorageDescriptor(buildStorageDescriptor(partition.getSd()));
        }

        if (partition.getParameters() != null) {
            partitionInput.withParameters(new HashMap<>(partition.getParameters()));
        }

        return partitionInput;
    }

    /**
     * Normalizes S3 location paths by translating s3a:// and s3n:// to s3://.
     *
     * @param location the original location string
     * @return the normalized location with s3:// prefix, or the original if not an s3a/s3n path
     */
    public static String translateLocation(String location) {
        if (location == null) {
            return null;
        }
        if (location.startsWith("s3a://")) {
            return "s3://" + location.substring(6);
        }
        if (location.startsWith("s3n://")) {
            return "s3://" + location.substring(6);
        }
        return location;
    }
}
