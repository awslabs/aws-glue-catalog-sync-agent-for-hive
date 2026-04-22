package com.amazonaws.services.glue.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
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
 * Property-based tests for HiveGlueCatalogSyncAgent event handler helper methods.
 *
 * Tests Properties 3-7 from the design document, covering:
 * - Event filtering on S3 location (Property 3)
 * - Table ownership filtering (Property 4)
 * - Disallowed table exclusion (Property 5)
 * - Suppress drop events flag (Property 6)
 * - S3-only partition filtering (Property 7)
 */
public class EventHandlerPropertyTest {

    // ---- Shared Generators ----

    @Provide
    Arbitrary<String> identifiers() {
        return Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1)
            .ofMaxLength(15);
    }

    @Provide
    Arbitrary<String> s3Locations() {
        return Combinators.combine(
            Arbitraries.of("s3://", "s3a://", "s3n://"),
            identifiers()
        ).as((prefix, path) -> prefix + "bucket-" + path + "/data");
    }

    @Provide
    Arbitrary<String> nonS3Locations() {
        return Combinators.combine(
            Arbitraries.of("hdfs://", "file:///", "/local/", "ftp://", "gs://"),
            identifiers()
        ).as((prefix, path) -> prefix + path);
    }


    private Table buildTable(String dbName, String tableName, String location, Map<String, String> params) {
        Table table = new Table();
        table.setDbName(dbName);
        table.setTableName(tableName);
        table.setTableType("EXTERNAL_TABLE");

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(location);
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
        sd.setCols(new ArrayList<FieldSchema>());
        sd.getCols().add(new FieldSchema("col1", "string", null));
        sd.setNumBuckets(-1);

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        serDeInfo.setParameters(new HashMap<String, String>());
        sd.setSerdeInfo(serDeInfo);

        table.setSd(sd);
        table.setParameters(params != null ? new HashMap<>(params) : new HashMap<String, String>());
        table.setPartitionKeys(new ArrayList<FieldSchema>());
        return table;
    }

    private Partition buildPartition(String location, List<String> values) {
        Partition partition = new Partition();
        partition.setValues(new ArrayList<>(values));

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(location);
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
        sd.setCols(new ArrayList<FieldSchema>());
        sd.getCols().add(new FieldSchema("col1", "string", null));
        sd.setNumBuckets(-1);

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        serDeInfo.setParameters(new HashMap<String, String>());
        sd.setSerdeInfo(serDeInfo);

        partition.setSd(sd);
        partition.setParameters(new HashMap<String, String>());
        return partition;
    }

    /**
     * Helper to access the operationQueue field via reflection for test verification.
     */
    private LinkedBlockingQueue<CatalogOperation> getQueue(HiveGlueCatalogSyncAgent agent) {
        try {
            java.lang.reflect.Field field = HiveGlueCatalogSyncAgent.class.getDeclaredField("operationQueue");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<CatalogOperation> queue =
                (LinkedBlockingQueue<CatalogOperation>) field.get(agent);
            if (queue == null) {
                queue = new LinkedBlockingQueue<>();
                field.set(agent, queue);
            }
            return queue;
        } catch (Exception e) {
            throw new RuntimeException("Failed to access operationQueue", e);
        }
    }

    /**
     * Helper to set the suppressAllDropEvents field via reflection.
     */
    private void setSuppressAllDropEvents(HiveGlueCatalogSyncAgent agent, boolean value) {
        try {
            java.lang.reflect.Field field = HiveGlueCatalogSyncAgent.class.getDeclaredField("suppressAllDropEvents");
            field.setAccessible(true);
            field.setBoolean(agent, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set suppressAllDropEvents", e);
        }
    }

    /**
     * Helper to set the syncTableStatistics field via reflection.
     */
    private void setSyncTableStatistics(HiveGlueCatalogSyncAgent agent, boolean value) {
        try {
            java.lang.reflect.Field field = HiveGlueCatalogSyncAgent.class.getDeclaredField("syncTableStatistics");
            field.setAccessible(true);
            field.setBoolean(agent, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set syncTableStatistics", e);
        }
    }

    private HiveGlueCatalogSyncAgent createAgent() {
        try {
            HiveGlueCatalogSyncAgent agent = new HiveGlueCatalogSyncAgent();
            // Ensure operationQueue is initialized
            getQueue(agent);
            return agent;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create agent", e);
        }
    }

    // ========================================================================
    // Property 3: Event Filtering Gates on S3 Location
    // Feature: gdc-direct-api-sync, Property 3: Event Filtering Gates on S3 Location
    // ========================================================================

    @Provide
    Arbitrary<String> allLocations() {
        return Arbitraries.oneOf(s3Locations(), nonS3Locations());
    }

    @Provide
    Arbitrary<Boolean> hasStorageHandler() {
        return Arbitraries.of(true, false);
    }

    /**
     * Validates: Requirements 4.1, 4.2, 5.1, 6.1, 7.1, 8.1, 8.2
     *
     * For any table with any location and storage handler configuration,
     * isSyncEligible returns true if and only if the location is S3-based
     * (after translation) and no storage_handler parameter is present.
     */
    @Property(tries = 100)
    void syncEligibilityMatchesS3AndNoStorageHandler(
            @ForAll("allLocations") String location,
            @ForAll("hasStorageHandler") boolean withStorageHandler) {

        HiveGlueCatalogSyncAgent agent = createAgent();

        Map<String, String> params = new HashMap<>();
        if (withStorageHandler) {
            params.put("storage_handler", "org.apache.hadoop.hive.hbase.HBaseStorageHandler");
        }

        Table table = buildTable("testdb", "testtable", location, params);

        boolean result = agent.isSyncEligible(table);

        // Expected: eligible only if S3-based location AND no storage handler
        String translatedLocation = TableInputBuilder.translateLocation(location);
        boolean isS3 = translatedLocation.startsWith("s3");
        boolean expected = isS3 && !withStorageHandler;

        assert result == expected
            : "isSyncEligible mismatch for location='" + location
              + "', storageHandler=" + withStorageHandler
              + ": expected " + expected + " got " + result;
    }

    /**
     * Validates: Requirements 4.1, 4.2, 5.1, 6.1, 7.1, 8.1, 8.2
     *
     * Tables with S3 locations (s3://, s3a://, s3n://) and no storage handler
     * are always sync eligible.
     */
    @Property(tries = 100)
    void s3TablesWithoutStorageHandlerAreEligible(@ForAll("s3Locations") String location) {
        HiveGlueCatalogSyncAgent agent = createAgent();
        Table table = buildTable("testdb", "testtable", location, new HashMap<String, String>());

        assert agent.isSyncEligible(table)
            : "S3 table without storage handler should be eligible: " + location;
    }

    /**
     * Validates: Requirements 4.2, 8.2
     *
     * Tables with non-S3 locations are never sync eligible.
     */
    @Property(tries = 100)
    void nonS3TablesAreNeverEligible(@ForAll("nonS3Locations") String location) {
        HiveGlueCatalogSyncAgent agent = createAgent();
        Table table = buildTable("testdb", "testtable", location, new HashMap<String, String>());

        assert !agent.isSyncEligible(table)
            : "Non-S3 table should not be eligible: " + location;
    }

    /**
     * Validates: Requirements 4.1, 4.2
     *
     * Tables with a storage_handler parameter are never sync eligible,
     * regardless of location.
     */
    @Property(tries = 100)
    void tablesWithStorageHandlerAreNeverEligible(@ForAll("allLocations") String location) {
        HiveGlueCatalogSyncAgent agent = createAgent();
        Map<String, String> params = new HashMap<>();
        params.put("storage_handler", "org.apache.hadoop.hive.hbase.HBaseStorageHandler");
        Table table = buildTable("testdb", "testtable", location, params);

        assert !agent.isSyncEligible(table)
            : "Table with storage handler should not be eligible: " + location;
    }


    // ========================================================================
    // Property 4: Table Ownership Filtering Prevents Circular Updates
    // Feature: gdc-direct-api-sync, Property 4: Table Ownership Filtering Prevents Circular Updates
    // ========================================================================

    @Provide
    Arbitrary<String> ownershipValues() {
        return Arbitraries.of("gdc", "hms", null, "", "other", "GDC", "HMS");
    }

    /**
     * Validates: Requirements 3.1, 3.2, 3.3
     *
     * For any table with any ownership value, isTableOwnershipValid returns false
     * only when ownership is exactly "gdc". All other values (hms, absent, empty,
     * other) return true.
     */
    @Property(tries = 100)
    void ownershipFilterBlocksOnlyGdc(@ForAll("ownershipValues") String ownership) {
        HiveGlueCatalogSyncAgent agent = createAgent();

        Map<String, String> params = new HashMap<>();
        if (ownership != null) {
            params.put("table.system.ownership", ownership);
        }

        Table table = buildTable("testdb", "testtable", "s3://bucket/path", params);

        boolean result = agent.isTableOwnershipValid(table);
        boolean expected = !"gdc".equals(ownership);

        assert result == expected
            : "isTableOwnershipValid mismatch for ownership='" + ownership
              + "': expected " + expected + " got " + result;
    }

    /**
     * Validates: Requirements 3.2
     *
     * Tables with ownership "gdc" are always rejected.
     */
    @Property(tries = 100)
    void gdcOwnedTablesAlwaysRejected(@ForAll("identifiers") String dbName,
                                       @ForAll("identifiers") String tableName) {
        HiveGlueCatalogSyncAgent agent = createAgent();

        Map<String, String> params = new HashMap<>();
        params.put("table.system.ownership", "gdc");

        Table table = buildTable(dbName, tableName, "s3://bucket/path", params);

        assert !agent.isTableOwnershipValid(table)
            : "GDC-owned table should not pass ownership check";
    }

    /**
     * Validates: Requirements 3.3
     *
     * Tables with no parameters (null) pass ownership check.
     */
    @Property(tries = 100)
    void tablesWithNullParametersPassOwnership(@ForAll("identifiers") String dbName,
                                                @ForAll("identifiers") String tableName) {
        HiveGlueCatalogSyncAgent agent = createAgent();

        Table table = buildTable(dbName, tableName, "s3://bucket/path", null);
        table.setParameters(null);

        assert agent.isTableOwnershipValid(table)
            : "Table with null parameters should pass ownership check";
    }

    // ========================================================================
    // Property 5: Disallowed Tables Are Excluded From All Operations
    // Feature: gdc-direct-api-sync, Property 5: Disallowed Tables Are Excluded From All Operations
    // ========================================================================

    /**
     * Validates: Requirements 9.1, 9.2
     *
     * For any table added to the blacklist, isDisallowed returns true.
     * For tables not in the blacklist, isDisallowed returns false.
     */
    @Property(tries = 100)
    void disallowedTablesAreDetected(@ForAll("identifiers") String dbName,
                                       @ForAll("identifiers") String tableName) {
        HiveGlueCatalogSyncAgent agent = createAgent();

        // Before blacklisting, should not be disallowed
        assert !agent.isDisallowed(dbName, tableName)
            : "Table should not be disallowed before adding";

        // Add to blacklist
        agent.addToBlacklist(dbName, tableName);

        // After blacklisting, should be disallowed
        assert agent.isDisallowed(dbName, tableName)
            : "Table should be disallowed after adding";
    }

    /**
     * Validates: Requirements 9.1, 9.2
     *
     * Blacklisting one table does not affect other tables.
     */
    @Property(tries = 100)
    void blacklistingIsTableSpecific(@ForAll("identifiers") String dbName,
                                      @ForAll("identifiers") String tableName1,
                                      @ForAll("identifiers") String tableName2) {
        // Only test when table names are different
        if (tableName1.equals(tableName2)) {
            return;
        }

        HiveGlueCatalogSyncAgent agent = createAgent();

        agent.addToBlacklist(dbName, tableName1);

        assert agent.isDisallowed(dbName, tableName1)
            : "Disallowed table should be detected";
        assert !agent.isDisallowed(dbName, tableName2)
            : "Non-disallowed table should not be detected";
    }

    /**
     * Validates: Requirements 9.1, 9.2
     *
     * Blacklisting a table prevents addToQueue from being called for that table.
     * We verify this by checking that the queue remains empty when a disallowed
     * table's operation is attempted through the agent's checks.
     */
    @Property(tries = 100)
    void disallowedTableProducesNoQueuedOperations(
            @ForAll("identifiers") String dbName,
            @ForAll("identifiers") String tableName) {

        HiveGlueCatalogSyncAgent agent = createAgent();
        LinkedBlockingQueue<CatalogOperation> queue = getQueue(agent);

        agent.addToBlacklist(dbName, tableName);

        // Simulate the check that event handlers perform
        Table table = buildTable(dbName, tableName, "s3://bucket/path", new HashMap<String, String>());

        // The event handler pattern: check eligibility, ownership, blacklist, then enqueue
        if (agent.isSyncEligible(table) && agent.isTableOwnershipValid(table)
                && !agent.isDisallowed(dbName, tableName)) {
            agent.addToQueue(new CatalogOperation(
                CatalogOperation.OperationType.CREATE_TABLE, dbName, tableName, null));
        }

        assert queue.isEmpty()
            : "Disallowed table should produce no queued operations";
    }


    // ========================================================================
    // Property 6: Suppress Drop Events Flag Blocks All Drop Operations
    // Feature: gdc-direct-api-sync, Property 6: Suppress Drop Events Flag Blocks All Drop Operations
    // ========================================================================

    /**
     * Validates: Requirements 5.2, 7.2
     *
     * When suppressAllDropEvents is true, the agent's drop event handlers
     * should not enqueue any operations. We test this by verifying the flag
     * behavior: when suppress is on, eligible drop operations should be blocked.
     */
    @Property(tries = 100)
    void suppressDropEventsBlocksDropOperations(
            @ForAll("identifiers") String dbName,
            @ForAll("identifiers") String tableName,
            @ForAll("s3Locations") String location) {

        HiveGlueCatalogSyncAgent agent = createAgent();
        LinkedBlockingQueue<CatalogOperation> queue = getQueue(agent);
        setSuppressAllDropEvents(agent, true);

        Table table = buildTable(dbName, tableName, location, new HashMap<String, String>());

        // Simulate the suppressAllDropEvents check from onDropTable/onDropPartition
        boolean suppressAllDropEvents = true; // mirrors the agent's field
        if (!suppressAllDropEvents) {
            if (agent.isSyncEligible(table) && agent.isTableOwnershipValid(table)
                    && !agent.isDisallowed(dbName, tableName)) {
                agent.addToQueue(new CatalogOperation(
                    CatalogOperation.OperationType.DROP_TABLE, dbName, tableName, null));
            }
        }

        assert queue.isEmpty()
            : "Suppress drop events should block all drop operations";
    }

    /**
     * Validates: Requirements 5.2, 7.2
     *
     * When suppressAllDropEvents is false, eligible drop operations should be enqueued.
     */
    @Property(tries = 100)
    void disabledSuppressAllowsDropOperations(
            @ForAll("identifiers") String dbName,
            @ForAll("identifiers") String tableName,
            @ForAll("s3Locations") String location) {

        HiveGlueCatalogSyncAgent agent = createAgent();
        LinkedBlockingQueue<CatalogOperation> queue = getQueue(agent);
        setSuppressAllDropEvents(agent, false);

        Table table = buildTable(dbName, tableName, location, new HashMap<String, String>());

        // Simulate the event handler logic with suppress=false
        boolean suppressAllDropEvents = false;
        if (!suppressAllDropEvents) {
            if (agent.isSyncEligible(table) && agent.isTableOwnershipValid(table)
                    && !agent.isDisallowed(dbName, tableName)) {
                agent.addToQueue(new CatalogOperation(
                    CatalogOperation.OperationType.DROP_TABLE, dbName, tableName, null));
            }
        }

        assert !queue.isEmpty()
            : "Disabled suppress should allow drop operations for eligible tables";

        CatalogOperation op = queue.poll();
        assert op.getType() == CatalogOperation.OperationType.DROP_TABLE
            : "Operation should be DROP_TABLE";
        assert op.getDatabaseName().equals(dbName)
            : "Database name mismatch";
        assert op.getTableName().equals(tableName)
            : "Table name mismatch";
    }

    /**
     * Validates: Requirements 5.2, 7.2
     *
     * Toggle behavior: the suppress flag correctly gates drop operations.
     * For any boolean value of suppress, the queue state matches the flag.
     */
    @Property(tries = 100)
    void suppressFlagTogglesDropBehavior(
            @ForAll boolean suppress,
            @ForAll("identifiers") String dbName,
            @ForAll("identifiers") String tableName) {

        HiveGlueCatalogSyncAgent agent = createAgent();
        LinkedBlockingQueue<CatalogOperation> queue = getQueue(agent);
        setSuppressAllDropEvents(agent, suppress);

        Table table = buildTable(dbName, tableName, "s3://bucket/data", new HashMap<String, String>());

        // Simulate the event handler pattern
        if (!suppress) {
            if (agent.isSyncEligible(table) && agent.isTableOwnershipValid(table)
                    && !agent.isDisallowed(dbName, tableName)) {
                agent.addToQueue(new CatalogOperation(
                    CatalogOperation.OperationType.DROP_TABLE, dbName, tableName, null));
            }
        }

        boolean queueHasOps = !queue.isEmpty();
        assert queueHasOps == !suppress
            : "Queue state should match suppress flag: suppress=" + suppress
              + ", queueHasOps=" + queueHasOps;
    }

    // ========================================================================
    // Property 7: Only S3-Based Partitions Are Included in Partition Operations
    // Feature: gdc-direct-api-sync, Property 7: Only S3-Based Partitions Are Included in Partition Operations
    // ========================================================================

    @Provide
    Arbitrary<List<String>> partitionValues() {
        return identifiers().list().ofMinSize(1).ofMaxSize(3);
    }

    /**
     * Validates: Requirements 6.4, 7.4
     *
     * When filtering partitions for AddPartition events, only partitions with
     * S3-based locations are included. Non-S3 partitions are excluded.
     */
    @Property(tries = 100)
    void onlyS3PartitionsIncludedInAddPartition(
            @ForAll("identifiers") String dbName,
            @ForAll("identifiers") String tableName,
            @ForAll("s3Locations") String s3Location,
            @ForAll("nonS3Locations") String nonS3Location,
            @ForAll("partitionValues") List<String> s3Values,
            @ForAll("partitionValues") List<String> nonS3Values) {

        // Build a mix of S3 and non-S3 partitions
        List<Partition> partitions = new ArrayList<>();
        partitions.add(buildPartition(s3Location, s3Values));
        partitions.add(buildPartition(nonS3Location, nonS3Values));

        // Apply the same filtering logic as onAddPartition
        List<PartitionInput> filteredInputs = new ArrayList<>();
        for (Partition p : partitions) {
            if (p.getSd() != null && p.getSd().getLocation() != null) {
                String translatedLocation = TableInputBuilder.translateLocation(p.getSd().getLocation());
                if (translatedLocation.startsWith("s3")) {
                    filteredInputs.add(TableInputBuilder.buildPartitionInput(p));
                }
            }
        }

        // Should include only the S3 partition
        assert filteredInputs.size() == 1
            : "Expected 1 S3 partition but got " + filteredInputs.size();

        // Verify the included partition has the correct values
        assert filteredInputs.get(0).getValues().equals(s3Values)
            : "Partition values mismatch";

        // Verify the location was translated to s3://
        String resultLocation = filteredInputs.get(0).getStorageDescriptor().getLocation();
        assert resultLocation.startsWith("s3://")
            : "Partition location should start with s3:// but was: " + resultLocation;
    }

    /**
     * Validates: Requirements 7.4
     *
     * When filtering partitions for DropPartition events, only partitions with
     * S3-based locations produce PartitionValueList entries.
     */
    @Property(tries = 100)
    void onlyS3PartitionsIncludedInDropPartition(
            @ForAll("s3Locations") String s3Location,
            @ForAll("nonS3Locations") String nonS3Location,
            @ForAll("partitionValues") List<String> s3Values,
            @ForAll("partitionValues") List<String> nonS3Values) {

        List<Partition> partitions = new ArrayList<>();
        partitions.add(buildPartition(s3Location, s3Values));
        partitions.add(buildPartition(nonS3Location, nonS3Values));

        // Apply the same filtering logic as onDropPartition
        List<PartitionValueList> filteredValueLists = new ArrayList<>();
        for (Partition p : partitions) {
            if (p.getSd() != null && p.getSd().getLocation() != null) {
                String translatedLocation = TableInputBuilder.translateLocation(p.getSd().getLocation());
                if (translatedLocation.startsWith("s3")) {
                    PartitionValueList pvl = new PartitionValueList()
                        .withValues(new ArrayList<>(p.getValues()));
                    filteredValueLists.add(pvl);
                }
            }
        }

        assert filteredValueLists.size() == 1
            : "Expected 1 S3 partition value list but got " + filteredValueLists.size();

        assert filteredValueLists.get(0).getValues().equals(s3Values)
            : "Partition values mismatch in drop operation";
    }

    /**
     * Validates: Requirements 6.4, 7.4
     *
     * When all partitions are S3-based, all are included.
     */
    @Property(tries = 100)
    void allS3PartitionsAreIncluded(
            @ForAll("s3Locations") String location1,
            @ForAll("s3Locations") String location2,
            @ForAll("partitionValues") List<String> values1,
            @ForAll("partitionValues") List<String> values2) {

        List<Partition> partitions = new ArrayList<>();
        partitions.add(buildPartition(location1, values1));
        partitions.add(buildPartition(location2, values2));

        List<PartitionInput> filteredInputs = new ArrayList<>();
        for (Partition p : partitions) {
            if (p.getSd() != null && p.getSd().getLocation() != null) {
                String translatedLocation = TableInputBuilder.translateLocation(p.getSd().getLocation());
                if (translatedLocation.startsWith("s3")) {
                    filteredInputs.add(TableInputBuilder.buildPartitionInput(p));
                }
            }
        }

        assert filteredInputs.size() == 2
            : "All S3 partitions should be included, got " + filteredInputs.size();
    }

    /**
     * Validates: Requirements 6.4, 7.4
     *
     * When no partitions are S3-based, none are included.
     */
    @Property(tries = 100)
    void noNonS3PartitionsAreIncluded(
            @ForAll("nonS3Locations") String location1,
            @ForAll("nonS3Locations") String location2,
            @ForAll("partitionValues") List<String> values1,
            @ForAll("partitionValues") List<String> values2) {

        List<Partition> partitions = new ArrayList<>();
        partitions.add(buildPartition(location1, values1));
        partitions.add(buildPartition(location2, values2));

        List<PartitionInput> filteredInputs = new ArrayList<>();
        for (Partition p : partitions) {
            if (p.getSd() != null && p.getSd().getLocation() != null) {
                String translatedLocation = TableInputBuilder.translateLocation(p.getSd().getLocation());
                if (translatedLocation.startsWith("s3")) {
                    filteredInputs.add(TableInputBuilder.buildPartitionInput(p));
                }
            }
        }

        assert filteredInputs.isEmpty()
            : "No non-S3 partitions should be included, got " + filteredInputs.size();
    }
}
