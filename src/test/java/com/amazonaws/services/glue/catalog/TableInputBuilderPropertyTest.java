package com.amazonaws.services.glue.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.TableInput;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * Property-based tests for TableInputBuilder.
 *
 * Feature: gdc-direct-api-sync, Property 1: Hive-to-Glue Table Conversion Preserves Metadata
 * Validates: Requirements 2.1, 2.2, 2.3, 2.5, 2.6
 */
public class TableInputBuilderPropertyTest {

    // ---- Generators ----

    @Provide
    Arbitrary<String> identifiers() {
        return Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1)
            .ofMaxLength(20);
    }

    @Provide
    Arbitrary<String> hiveTypes() {
        return Arbitraries.of(
            "string", "int", "bigint", "double", "float",
            "boolean", "timestamp", "date", "binary",
            "array<string>", "map<string,int>", "struct<a:int,b:string>"
        );
    }

    @Provide
    Arbitrary<String> comments() {
        return Arbitraries.of("", "a comment", "column description", null);
    }

    @Provide
    Arbitrary<FieldSchema> fieldSchemas() {
        return Combinators.combine(identifiers(), hiveTypes(), comments())
            .as((name, type, comment) -> new FieldSchema(name, type, comment));
    }

    @Provide
    Arbitrary<String> s3Locations() {
        return identifiers().map(bucket -> "s3://bucket-" + bucket + "/path/to/data");
    }

    @Provide
    Arbitrary<Map<String, String>> serdeParameters() {
        return Arbitraries.maps(identifiers(), identifiers())
            .ofMinSize(0).ofMaxSize(5);
    }

    @Provide
    Arbitrary<String> serdeLibraries() {
        return Arbitraries.of(
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
            "org.apache.hadoop.hive.serde2.OpenCSVSerde"
        );
    }

    @Provide
    Arbitrary<String> inputFormats() {
        return Arbitraries.of(
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
        );
    }

    @Provide
    Arbitrary<String> outputFormats() {
        return Arbitraries.of(
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
        );
    }

    @Provide
    Arbitrary<Map<String, String>> tableParameters() {
        return Arbitraries.maps(
            Arbitraries.of("custom.key1", "custom.key2", "custom.key3", "EXTERNAL", "comment"),
            identifiers()
        ).ofMinSize(0).ofMaxSize(5);
    }

    @Provide
    Arbitrary<StorageDescriptor> storageDescriptors() {
        return Combinators.combine(
            fieldSchemas().list().ofMinSize(1).ofMaxSize(10).map(l -> new ArrayList<>(l)),
            inputFormats(),
            outputFormats(),
            serdeLibraries(),
            serdeParameters(),
            s3Locations()
        ).as((columns, inputFmt, outputFmt, serdeLib, serdeParams, location) -> {
            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(columns);
            sd.setInputFormat(inputFmt);
            sd.setOutputFormat(outputFmt);
            sd.setLocation(location);
            sd.setNumBuckets(-1);

            SerDeInfo serDeInfo = new SerDeInfo();
            serDeInfo.setSerializationLib(serdeLib);
            serDeInfo.setParameters(new HashMap<>(serdeParams));
            sd.setSerdeInfo(serDeInfo);

            return sd;
        });
    }

    @Provide
    Arbitrary<Table> hiveTables() {
        return Combinators.combine(
            identifiers(),
            identifiers(),
            storageDescriptors(),
            fieldSchemas().list().ofMinSize(0).ofMaxSize(3).map(l -> new ArrayList<>(l)),
            tableParameters()
        ).as((tableName, dbName, sd, partitionKeys, params) -> {
            Table table = new Table();
            table.setTableName(tableName);
            table.setDbName(dbName);
            table.setTableType("EXTERNAL_TABLE");
            table.setSd(sd);
            table.setPartitionKeys(partitionKeys);
            table.setParameters(new HashMap<>(params));
            return table;
        });
    }

    // ---- Property Tests ----

    // Feature: gdc-direct-api-sync, Property 1: Hive-to-Glue Table Conversion Preserves Metadata
    /**
     * Validates: Requirements 2.1
     * Each Hive column's name, type, and comment matches the corresponding Glue Column.
     */
    @Property(tries = 100)
    void columnsArePreserved(@ForAll("hiveTables") Table hiveTable) {
        TableInput result = TableInputBuilder.buildTableInput(hiveTable, true);

        List<FieldSchema> hiveCols = hiveTable.getSd().getCols();
        List<Column> glueCols = result.getStorageDescriptor().getColumns();

        assert glueCols.size() == hiveCols.size()
            : "Column count mismatch: expected " + hiveCols.size() + " got " + glueCols.size();

        for (int i = 0; i < hiveCols.size(); i++) {
            FieldSchema hiveCol = hiveCols.get(i);
            Column glueCol = glueCols.get(i);

            assert hiveCol.getName().equals(glueCol.getName())
                : "Column name mismatch at index " + i;
            assert hiveCol.getType().equals(glueCol.getType())
                : "Column type mismatch at index " + i;
            if (hiveCol.getComment() != null) {
                assert hiveCol.getComment().equals(glueCol.getComment())
                    : "Column comment mismatch at index " + i;
            }
        }
    }

    // Feature: gdc-direct-api-sync, Property 1: Hive-to-Glue Table Conversion Preserves Metadata
    /**
     * Validates: Requirements 2.2
     * The Glue StorageDescriptor's inputFormat, outputFormat, serdeInfo, and location
     * match the Hive StorageDescriptor.
     */
    @Property(tries = 100)
    void storageDescriptorIsPreserved(@ForAll("hiveTables") Table hiveTable) {
        TableInput result = TableInputBuilder.buildTableInput(hiveTable, true);

        StorageDescriptor hiveSd = hiveTable.getSd();
        com.amazonaws.services.glue.model.StorageDescriptor glueSd = result.getStorageDescriptor();

        assert hiveSd.getInputFormat().equals(glueSd.getInputFormat())
            : "InputFormat mismatch";
        assert hiveSd.getOutputFormat().equals(glueSd.getOutputFormat())
            : "OutputFormat mismatch";

        // Location should be translated (s3a/s3n -> s3)
        String expectedLocation = TableInputBuilder.translateLocation(hiveSd.getLocation());
        assert expectedLocation.equals(glueSd.getLocation())
            : "Location mismatch: expected " + expectedLocation + " got " + glueSd.getLocation();

        // SerDe info
        assert hiveSd.getSerdeInfo().getSerializationLib()
            .equals(glueSd.getSerdeInfo().getSerializationLibrary())
            : "SerDe library mismatch";

        if (hiveSd.getSerdeInfo().getParameters() != null) {
            assert hiveSd.getSerdeInfo().getParameters()
                .equals(glueSd.getSerdeInfo().getParameters())
                : "SerDe parameters mismatch";
        }
    }

    // Feature: gdc-direct-api-sync, Property 1: Hive-to-Glue Table Conversion Preserves Metadata
    /**
     * Validates: Requirements 2.3
     * Partition keys are correctly mapped to Glue Column objects.
     */
    @Property(tries = 100)
    void partitionKeysArePreserved(@ForAll("hiveTables") Table hiveTable) {
        TableInput result = TableInputBuilder.buildTableInput(hiveTable, true);

        List<FieldSchema> hivePartKeys = hiveTable.getPartitionKeys();

        if (hivePartKeys == null || hivePartKeys.isEmpty()) {
            assert result.getPartitionKeys() == null || result.getPartitionKeys().isEmpty()
                : "Expected no partition keys but got some";
            return;
        }

        List<Column> gluePartKeys = result.getPartitionKeys();
        assert gluePartKeys.size() == hivePartKeys.size()
            : "Partition key count mismatch";

        for (int i = 0; i < hivePartKeys.size(); i++) {
            FieldSchema hiveKey = hivePartKeys.get(i);
            Column glueKey = gluePartKeys.get(i);

            assert hiveKey.getName().equals(glueKey.getName())
                : "Partition key name mismatch at index " + i;
            assert hiveKey.getType().equals(glueKey.getType())
                : "Partition key type mismatch at index " + i;
            if (hiveKey.getComment() != null) {
                assert hiveKey.getComment().equals(glueKey.getComment())
                    : "Partition key comment mismatch at index " + i;
            }
        }
    }

    // Feature: gdc-direct-api-sync, Property 1: Hive-to-Glue Table Conversion Preserves Metadata
    /**
     * Validates: Requirements 2.5, 2.6
     * Table parameters are preserved in the Glue TableInput.
     */
    @Property(tries = 100)
    void tableParametersArePreserved(@ForAll("hiveTables") Table hiveTable) {
        TableInput result = TableInputBuilder.buildTableInput(hiveTable, true);

        Map<String, String> hiveParams = hiveTable.getParameters();

        if (hiveParams == null || hiveParams.isEmpty()) {
            // Parameters may be null or empty
            return;
        }

        Map<String, String> glueParams = result.getParameters();
        assert glueParams != null : "Glue parameters should not be null when Hive has parameters";

        for (Map.Entry<String, String> entry : hiveParams.entrySet()) {
            assert glueParams.containsKey(entry.getKey())
                : "Missing parameter key: " + entry.getKey();
            assert entry.getValue().equals(glueParams.get(entry.getKey()))
                : "Parameter value mismatch for key: " + entry.getKey();
        }
    }

    // Feature: gdc-direct-api-sync, Property 1: Hive-to-Glue Table Conversion Preserves Metadata
    /**
     * Validates: Requirements 2.1, 2.2, 2.3, 2.5, 2.6
     * Combined property: the full conversion preserves all metadata fields together.
     */
    @Property(tries = 100)
    void fullConversionPreservesAllMetadata(@ForAll("hiveTables") Table hiveTable) {
        TableInput result = TableInputBuilder.buildTableInput(hiveTable, true);

        // Table name
        assert hiveTable.getTableName().equals(result.getName())
            : "Table name mismatch";

        // Table type
        assert hiveTable.getTableType().equals(result.getTableType())
            : "Table type mismatch";

        // Columns count
        assert hiveTable.getSd().getCols().size()
            == result.getStorageDescriptor().getColumns().size()
            : "Column count mismatch";

        // Partition keys count
        int expectedPartKeys = (hiveTable.getPartitionKeys() != null)
            ? hiveTable.getPartitionKeys().size() : 0;
        int actualPartKeys = (result.getPartitionKeys() != null)
            ? result.getPartitionKeys().size() : 0;
        assert expectedPartKeys == actualPartKeys
            : "Partition key count mismatch";

        // Storage descriptor fields
        assert hiveTable.getSd().getInputFormat()
            .equals(result.getStorageDescriptor().getInputFormat())
            : "InputFormat mismatch";
        assert hiveTable.getSd().getOutputFormat()
            .equals(result.getStorageDescriptor().getOutputFormat())
            : "OutputFormat mismatch";

        // Location (translated)
        String expectedLoc = TableInputBuilder.translateLocation(hiveTable.getSd().getLocation());
        assert expectedLoc.equals(result.getStorageDescriptor().getLocation())
            : "Location mismatch";

        // SerDe
        assert hiveTable.getSd().getSerdeInfo().getSerializationLib()
            .equals(result.getStorageDescriptor().getSerdeInfo().getSerializationLibrary())
            : "SerDe library mismatch";
    }

    // ---- Property 2 Generators ----

    @Provide
    Arbitrary<String> s3PathRemainders() {
        // Generate random bucket + path portions like "my-bucket/some/path/data.parquet"
        return Combinators.combine(
            Arbitraries.strings().withCharRange('a', 'z').ofMinLength(1).ofMaxLength(15),
            Arbitraries.strings().withCharRange('a', 'z').ofMinLength(0).ofMaxLength(30)
        ).as((bucket, path) -> bucket + "/" + path);
    }

    @Provide
    Arbitrary<String> s3aLocations() {
        return s3PathRemainders().map(remainder -> "s3a://" + remainder);
    }

    @Provide
    Arbitrary<String> s3nLocations() {
        return s3PathRemainders().map(remainder -> "s3n://" + remainder);
    }

    @Provide
    Arbitrary<String> s3StandardLocations() {
        return s3PathRemainders().map(remainder -> "s3://" + remainder);
    }

    @Provide
    Arbitrary<String> nonS3Locations() {
        return Combinators.combine(
            Arbitraries.of("hdfs://", "file://", "ftp://", "gs://", "wasb://", "/local/path/"),
            Arbitraries.strings().withCharRange('a', 'z').ofMinLength(1).ofMaxLength(30)
        ).as((prefix, path) -> prefix + path);
    }

    // ---- Property 2 Tests ----

    // Feature: gdc-direct-api-sync, Property 2: S3 Path Translation Normalizes All Variants
    /**
     * Validates: Requirements 2.4
     * For any S3 path with s3a:// prefix, translateLocation returns s3:// with remainder unchanged.
     */
    @Property(tries = 100)
    void s3aPathTranslationNormalizesToS3(@ForAll("s3aLocations") String s3aPath) {
        String result = TableInputBuilder.translateLocation(s3aPath);

        assert result.startsWith("s3://") : "Result should start with s3:// but was: " + result;

        String expectedRemainder = s3aPath.substring("s3a://".length());
        String actualRemainder = result.substring("s3://".length());
        assert expectedRemainder.equals(actualRemainder)
            : "Remainder changed: expected '" + expectedRemainder + "' got '" + actualRemainder + "'";
    }

    // Feature: gdc-direct-api-sync, Property 2: S3 Path Translation Normalizes All Variants
    /**
     * Validates: Requirements 2.4
     * For any S3 path with s3n:// prefix, translateLocation returns s3:// with remainder unchanged.
     */
    @Property(tries = 100)
    void s3nPathTranslationNormalizesToS3(@ForAll("s3nLocations") String s3nPath) {
        String result = TableInputBuilder.translateLocation(s3nPath);

        assert result.startsWith("s3://") : "Result should start with s3:// but was: " + result;

        String expectedRemainder = s3nPath.substring("s3n://".length());
        String actualRemainder = result.substring("s3://".length());
        assert expectedRemainder.equals(actualRemainder)
            : "Remainder changed: expected '" + expectedRemainder + "' got '" + actualRemainder + "'";
    }

    // Feature: gdc-direct-api-sync, Property 2: S3 Path Translation Normalizes All Variants
    /**
     * Validates: Requirements 2.4
     * For any path already starting with s3://, translateLocation returns the input unchanged.
     */
    @Property(tries = 100)
    void s3StandardPathIsUnchanged(@ForAll("s3StandardLocations") String s3Path) {
        String result = TableInputBuilder.translateLocation(s3Path);

        assert s3Path.equals(result)
            : "s3:// path should be unchanged but was: " + result;
    }

    // Feature: gdc-direct-api-sync, Property 2: S3 Path Translation Normalizes All Variants
    /**
     * Validates: Requirements 2.4
     * For any non-S3 path, translateLocation returns the input unchanged.
     */
    @Property(tries = 100)
    void nonS3PathIsUnchanged(@ForAll("nonS3Locations") String nonS3Path) {
        String result = TableInputBuilder.translateLocation(nonS3Path);

        assert nonS3Path.equals(result)
            : "Non-S3 path should be unchanged but was: " + result;
    }

}
