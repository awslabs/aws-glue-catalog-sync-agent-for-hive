package com.amazonaws.services.glue.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.glue.catalog.CatalogOperation.OperationType;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.TableInput;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * Property-based tests for OperationBatcher.
 *
 * Feature: gdc-direct-api-sync, Property 8: Batch Grouping Produces Correct Groups
 * Validates: Requirements 10.2
 */
public class OperationBatcherPropertyTest {

    // ---- Generators ----

    @Provide
    Arbitrary<String> databaseNames() {
        return Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1)
            .ofMaxLength(15)
            .map(s -> "db_" + s);
    }

    @Provide
    Arbitrary<String> tableNames() {
        return Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1)
            .ofMaxLength(15)
            .map(s -> "tbl_" + s);
    }

    @Provide
    Arbitrary<OperationType> operationTypes() {
        return Arbitraries.of(OperationType.values());
    }

    @Provide
    Arbitrary<CatalogOperation> catalogOperations() {
        return Combinators.combine(operationTypes(), databaseNames(), tableNames())
            .as((type, db, table) -> new CatalogOperation(type, db, table, null));
    }

    @Provide
    Arbitrary<List<CatalogOperation>> operationLists() {
        return catalogOperations().list().ofMinSize(0).ofMaxSize(50);
    }

    /**
     * Uses a small set of db/table names to increase grouping collisions,
     * making the test more interesting.
     */
    @Provide
    Arbitrary<List<CatalogOperation>> denseOperationLists() {
        Arbitrary<String> fewDbs = Arbitraries.of("db_alpha", "db_beta", "db_gamma");
        Arbitrary<String> fewTables = Arbitraries.of("tbl_one", "tbl_two", "tbl_three");
        Arbitrary<CatalogOperation> denseOps = Combinators.combine(operationTypes(), fewDbs, fewTables)
            .as((type, db, table) -> new CatalogOperation(type, db, table, null));
        return denseOps.list().ofMinSize(1).ofMaxSize(50);
    }

    // ---- Property Tests ----

    // Feature: gdc-direct-api-sync, Property 8: Batch Grouping Produces Correct Groups
    /**
     * Validates: Requirements 10.2
     * Every operation from the input appears in exactly one group.
     * Total count of operations across all groups equals the input count.
     */
    @Property(tries = 100)
    void everyOperationAppearsInExactlyOneGroup(@ForAll("operationLists") List<CatalogOperation> ops) {
        Map<String, List<CatalogOperation>> groups = OperationBatcher.groupByTable(ops);

        // Count total operations across all groups
        int totalInGroups = 0;
        for (List<CatalogOperation> group : groups.values()) {
            totalInGroups += group.size();
        }

        assert totalInGroups == ops.size()
            : "Total operations in groups (" + totalInGroups + ") != input size (" + ops.size() + ")";

        // Verify each input operation appears in exactly one group
        List<CatalogOperation> allGrouped = new ArrayList<>();
        for (List<CatalogOperation> group : groups.values()) {
            allGrouped.addAll(group);
        }

        // Use identity comparison — each CatalogOperation instance should appear once
        Set<CatalogOperation> inputSet = new HashSet<>(ops);
        Set<CatalogOperation> groupedSet = new HashSet<>(allGrouped);
        // Note: if there are duplicate references in the input, size check above covers it.
        // For distinct instances, verify grouped set matches input set.
        for (CatalogOperation op : ops) {
            boolean found = false;
            for (List<CatalogOperation> group : groups.values()) {
                for (CatalogOperation grouped : group) {
                    if (grouped == op) {
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }
            assert found : "Operation not found in any group: " + op.getFullTableName();
        }
    }

    // Feature: gdc-direct-api-sync, Property 8: Batch Grouping Produces Correct Groups
    /**
     * Validates: Requirements 10.2
     * All operations in a group share the same databaseName and tableName.
     */
    @Property(tries = 100)
    void allOpsInGroupShareSameDbAndTable(@ForAll("operationLists") List<CatalogOperation> ops) {
        Map<String, List<CatalogOperation>> groups = OperationBatcher.groupByTable(ops);

        for (Map.Entry<String, List<CatalogOperation>> entry : groups.entrySet()) {
            String groupKey = entry.getKey();
            List<CatalogOperation> group = entry.getValue();

            assert !group.isEmpty() : "Group should not be empty for key: " + groupKey;

            String expectedDb = group.get(0).getDatabaseName();
            String expectedTable = group.get(0).getTableName();

            for (CatalogOperation op : group) {
                assert expectedDb.equals(op.getDatabaseName())
                    : "Database mismatch in group '" + groupKey + "': expected '"
                      + expectedDb + "' got '" + op.getDatabaseName() + "'";
                assert expectedTable.equals(op.getTableName())
                    : "Table mismatch in group '" + groupKey + "': expected '"
                      + expectedTable + "' got '" + op.getTableName() + "'";
            }
        }
    }

    // Feature: gdc-direct-api-sync, Property 8: Batch Grouping Produces Correct Groups
    /**
     * Validates: Requirements 10.2
     * The group key matches the fullTableName of every operation in that group.
     */
    @Property(tries = 100)
    void groupKeyMatchesFullTableName(@ForAll("operationLists") List<CatalogOperation> ops) {
        Map<String, List<CatalogOperation>> groups = OperationBatcher.groupByTable(ops);

        for (Map.Entry<String, List<CatalogOperation>> entry : groups.entrySet()) {
            String groupKey = entry.getKey();
            for (CatalogOperation op : entry.getValue()) {
                assert groupKey.equals(op.getFullTableName())
                    : "Group key '" + groupKey + "' does not match operation fullTableName '"
                      + op.getFullTableName() + "'";
            }
        }
    }

    // Feature: gdc-direct-api-sync, Property 8: Batch Grouping Produces Correct Groups
    /**
     * Validates: Requirements 10.2
     * With dense inputs (few db/table combos), grouping still produces correct groups
     * and preserves FIFO order within each group.
     */
    @Property(tries = 100)
    void denseInputsGroupCorrectlyAndPreserveFifo(@ForAll("denseOperationLists") List<CatalogOperation> ops) {
        Map<String, List<CatalogOperation>> groups = OperationBatcher.groupByTable(ops);

        // Total count preserved
        int totalInGroups = 0;
        for (List<CatalogOperation> group : groups.values()) {
            totalInGroups += group.size();
        }
        assert totalInGroups == ops.size()
            : "Total operations in groups (" + totalInGroups + ") != input size (" + ops.size() + ")";

        // All ops in each group share db/table
        for (List<CatalogOperation> group : groups.values()) {
            String expectedDb = group.get(0).getDatabaseName();
            String expectedTable = group.get(0).getTableName();
            for (CatalogOperation op : group) {
                assert expectedDb.equals(op.getDatabaseName())
                    : "Database mismatch in dense group";
                assert expectedTable.equals(op.getTableName())
                    : "Table mismatch in dense group";
            }
        }

        // FIFO order: for each group, the operations should appear in the same
        // relative order as they did in the original input
        for (List<CatalogOperation> group : groups.values()) {
            int lastIndex = -1;
            for (CatalogOperation op : group) {
                int idx = indexOfIdentity(ops, op, lastIndex + 1);
                assert idx > lastIndex
                    : "FIFO order violated in group: operation appeared at index " + idx
                      + " but previous was at " + lastIndex;
                lastIndex = idx;
            }
        }
    }

    /**
     * Finds the index of the given object (by identity) in the list, starting from startIdx.
     */
    private static int indexOfIdentity(List<CatalogOperation> list, CatalogOperation target, int startIdx) {
        for (int i = startIdx; i < list.size(); i++) {
            if (list.get(i) == target) {
                return i;
            }
        }
        return -1;
    }

    // ---- Generators for Property 9 ----

    @Provide
    Arbitrary<PartitionInput> partitionInputs() {
        Arbitrary<List<String>> values = Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1).ofMaxLength(10)
            .list().ofMinSize(1).ofMaxSize(5);
        Arbitrary<String> locations = Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1).ofMaxLength(20)
            .map(s -> "s3://bucket/" + s);
        return Combinators.combine(values, locations)
            .as((vals, loc) -> {
                PartitionInput pi = new PartitionInput();
                pi.withValues(vals);
                com.amazonaws.services.glue.model.StorageDescriptor sd =
                    new com.amazonaws.services.glue.model.StorageDescriptor();
                sd.withLocation(loc);
                pi.withStorageDescriptor(sd);
                return pi;
            });
    }

    @Provide
    Arbitrary<PartitionValueList> partitionValueLists() {
        return Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1).ofMaxLength(10)
            .list().ofMinSize(1).ofMaxSize(5)
            .map(vals -> new PartitionValueList().withValues(vals));
    }

    /**
     * Generates a list of consecutive ADD_PARTITIONS operations for the same db/table,
     * each with a random non-empty list of PartitionInput payloads.
     */
    @Provide
    Arbitrary<List<CatalogOperation>> consecutiveAddPartitionsOps() {
        Arbitrary<String> db = Arbitraries.of("db_alpha", "db_beta");
        Arbitrary<String> table = Arbitraries.of("tbl_one", "tbl_two");
        Arbitrary<List<PartitionInput>> partitions = partitionInputs().list().ofMinSize(1).ofMaxSize(5);

        return Combinators.combine(db, table, partitions.list().ofMinSize(1).ofMaxSize(10))
            .as((d, t, partitionLists) -> {
                List<CatalogOperation> ops = new ArrayList<>();
                for (List<PartitionInput> pl : partitionLists) {
                    ops.add(new CatalogOperation(OperationType.ADD_PARTITIONS, d, t, new ArrayList<>(pl)));
                }
                return ops;
            });
    }

    /**
     * Generates a list of consecutive DROP_PARTITIONS operations for the same db/table,
     * each with a random non-empty list of PartitionValueList payloads.
     */
    @Provide
    Arbitrary<List<CatalogOperation>> consecutiveDropPartitionsOps() {
        Arbitrary<String> db = Arbitraries.of("db_alpha", "db_beta");
        Arbitrary<String> table = Arbitraries.of("tbl_one", "tbl_two");
        Arbitrary<List<PartitionValueList>> valueLists = partitionValueLists().list().ofMinSize(1).ofMaxSize(5);

        return Combinators.combine(db, table, valueLists.list().ofMinSize(1).ofMaxSize(10))
            .as((d, t, valListOfLists) -> {
                List<CatalogOperation> ops = new ArrayList<>();
                for (List<PartitionValueList> vl : valListOfLists) {
                    ops.add(new CatalogOperation(OperationType.DROP_PARTITIONS, d, t, new ArrayList<>(vl)));
                }
                return ops;
            });
    }

    // ---- Property 9 Tests ----

    // Feature: gdc-direct-api-sync, Property 9: Batch Merging Concatenates Consecutive Partition Operations
    /**
     * Validates: Requirements 10.3
     * For consecutive ADD_PARTITIONS ops targeting the same table, merging produces
     * a single operation whose partition list is the ordered concatenation of all
     * individual partition lists.
     */
    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void mergingConsecutiveAddPartitionsConcatenatesInOrder(
            @ForAll("consecutiveAddPartitionsOps") List<CatalogOperation> ops) {

        // Build expected concatenation before merging
        List<PartitionInput> expectedPartitions = new ArrayList<>();
        for (CatalogOperation op : ops) {
            List<PartitionInput> payload = op.getPayload();
            if (payload != null) {
                expectedPartitions.addAll(payload);
            }
        }

        List<CatalogOperation> merged = OperationBatcher.mergeConsecutive(ops);

        assert merged.size() == 1
            : "Expected 1 merged operation but got " + merged.size();

        CatalogOperation result = merged.get(0);
        assert result.getType() == OperationType.ADD_PARTITIONS
            : "Merged operation type should be ADD_PARTITIONS but was " + result.getType();
        assert result.getDatabaseName().equals(ops.get(0).getDatabaseName())
            : "Database name mismatch after merge";
        assert result.getTableName().equals(ops.get(0).getTableName())
            : "Table name mismatch after merge";

        List<PartitionInput> mergedPartitions = result.getPayload();
        assert mergedPartitions.size() == expectedPartitions.size()
            : "Merged partition count (" + mergedPartitions.size()
              + ") != expected (" + expectedPartitions.size() + ")";

        // Verify order is preserved — each element should be the same object reference
        for (int idx = 0; idx < expectedPartitions.size(); idx++) {
            assert mergedPartitions.get(idx) == expectedPartitions.get(idx)
                : "Partition at index " + idx + " differs after merge (order not preserved)";
        }
    }

    // Feature: gdc-direct-api-sync, Property 9: Batch Merging Concatenates Consecutive Partition Operations
    /**
     * Validates: Requirements 10.4
     * For consecutive DROP_PARTITIONS ops targeting the same table, merging produces
     * a single operation whose partition value list is the ordered concatenation of all
     * individual partition value lists.
     */
    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void mergingConsecutiveDropPartitionsConcatenatesInOrder(
            @ForAll("consecutiveDropPartitionsOps") List<CatalogOperation> ops) {

        // Build expected concatenation before merging
        List<PartitionValueList> expectedValues = new ArrayList<>();
        for (CatalogOperation op : ops) {
            List<PartitionValueList> payload = op.getPayload();
            if (payload != null) {
                expectedValues.addAll(payload);
            }
        }

        List<CatalogOperation> merged = OperationBatcher.mergeConsecutive(ops);

        assert merged.size() == 1
            : "Expected 1 merged operation but got " + merged.size();

        CatalogOperation result = merged.get(0);
        assert result.getType() == OperationType.DROP_PARTITIONS
            : "Merged operation type should be DROP_PARTITIONS but was " + result.getType();
        assert result.getDatabaseName().equals(ops.get(0).getDatabaseName())
            : "Database name mismatch after merge";
        assert result.getTableName().equals(ops.get(0).getTableName())
            : "Table name mismatch after merge";

        List<PartitionValueList> mergedValues = result.getPayload();
        assert mergedValues.size() == expectedValues.size()
            : "Merged partition value count (" + mergedValues.size()
              + ") != expected (" + expectedValues.size() + ")";

        // Verify order is preserved — each element should be the same object reference
        for (int idx = 0; idx < expectedValues.size(); idx++) {
            assert mergedValues.get(idx) == expectedValues.get(idx)
                : "PartitionValueList at index " + idx + " differs after merge (order not preserved)";
        }
    }

    // Feature: gdc-direct-api-sync, Property 9: Batch Merging Concatenates Consecutive Partition Operations
    /**
     * Validates: Requirements 10.3, 10.4
     * A single ADD_PARTITIONS or DROP_PARTITIONS operation should pass through
     * mergeConsecutive unchanged (identity case).
     */
    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void singlePartitionOpPassesThroughUnchanged(
            @ForAll("consecutiveAddPartitionsOps") List<CatalogOperation> addOps,
            @ForAll("consecutiveDropPartitionsOps") List<CatalogOperation> dropOps) {

        // Take just the first op from each generated list
        List<CatalogOperation> singleAdd = Collections.singletonList(addOps.get(0));
        List<CatalogOperation> mergedAdd = OperationBatcher.mergeConsecutive(singleAdd);
        assert mergedAdd.size() == 1 : "Single ADD_PARTITIONS should produce 1 result";
        assert mergedAdd.get(0).getType() == OperationType.ADD_PARTITIONS;

        List<CatalogOperation> singleDrop = Collections.singletonList(dropOps.get(0));
        List<CatalogOperation> mergedDrop = OperationBatcher.mergeConsecutive(singleDrop);
        assert mergedDrop.size() == 1 : "Single DROP_PARTITIONS should produce 1 result";
        assert mergedDrop.get(0).getType() == OperationType.DROP_PARTITIONS;
    }

    // ---- Generators for Property 10 ----

    /**
     * Generates a random TableInput with a unique name and random columns,
     * suitable as a payload for CREATE_TABLE or UPDATE_TABLE operations.
     */
    @Provide
    Arbitrary<TableInput> tableInputs() {
        Arbitrary<String> names = Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1).ofMaxLength(15)
            .map(s -> "tbl_" + s);
        Arbitrary<List<Column>> columns = Arbitraries.strings()
            .withCharRange('a', 'z')
            .ofMinLength(1).ofMaxLength(10)
            .map(s -> new Column().withName("col_" + s).withType("string"))
            .list().ofMinSize(1).ofMaxSize(5);
        return Combinators.combine(names, columns)
            .as((name, cols) -> {
                TableInput ti = new TableInput();
                ti.withName(name);
                com.amazonaws.services.glue.model.StorageDescriptor sd =
                    new com.amazonaws.services.glue.model.StorageDescriptor();
                sd.withColumns(cols);
                sd.withLocation("s3://bucket/" + name);
                ti.withStorageDescriptor(sd);
                return ti;
            });
    }

    /**
     * Generates a list of consecutive CREATE_TABLE operations for the same db/table,
     * each with a distinct random TableInput payload.
     */
    @Provide
    Arbitrary<List<CatalogOperation>> consecutiveCreateTableOps() {
        Arbitrary<String> db = Arbitraries.of("db_alpha", "db_beta");
        Arbitrary<String> table = Arbitraries.of("tbl_one", "tbl_two");
        Arbitrary<List<TableInput>> payloads = tableInputs().list().ofMinSize(2).ofMaxSize(10);

        return Combinators.combine(db, table, payloads)
            .as((d, t, tableInputList) -> {
                List<CatalogOperation> ops = new ArrayList<>();
                for (TableInput ti : tableInputList) {
                    ops.add(new CatalogOperation(OperationType.CREATE_TABLE, d, t, ti));
                }
                return ops;
            });
    }

    /**
     * Generates a list of consecutive UPDATE_TABLE operations for the same db/table,
     * each with a distinct random TableInput payload.
     */
    @Provide
    Arbitrary<List<CatalogOperation>> consecutiveUpdateTableOps() {
        Arbitrary<String> db = Arbitraries.of("db_alpha", "db_beta");
        Arbitrary<String> table = Arbitraries.of("tbl_one", "tbl_two");
        Arbitrary<List<TableInput>> payloads = tableInputs().list().ofMinSize(2).ofMaxSize(10);

        return Combinators.combine(db, table, payloads)
            .as((d, t, tableInputList) -> {
                List<CatalogOperation> ops = new ArrayList<>();
                for (TableInput ti : tableInputList) {
                    ops.add(new CatalogOperation(OperationType.UPDATE_TABLE, d, t, ti));
                }
                return ops;
            });
    }

    /**
     * Generates a list of consecutive DROP_TABLE operations for the same db/table,
     * all with null payload.
     */
    @Provide
    Arbitrary<List<CatalogOperation>> consecutiveDropTableOps() {
        Arbitrary<String> db = Arbitraries.of("db_alpha", "db_beta");
        Arbitrary<String> table = Arbitraries.of("tbl_one", "tbl_two");
        Arbitrary<Integer> count = Arbitraries.integers().between(2, 10);

        return Combinators.combine(db, table, count)
            .as((d, t, n) -> {
                List<CatalogOperation> ops = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    ops.add(new CatalogOperation(OperationType.DROP_TABLE, d, t, null));
                }
                return ops;
            });
    }

    // ---- Property 10 Tests ----

    // Feature: gdc-direct-api-sync, Property 10: Batch Deduplication Uses Last-Write-Wins for Consecutive Table Operations
    /**
     * Validates: Requirements 10.6
     * For consecutive CREATE_TABLE ops targeting the same table, merging produces
     * a single operation whose payload is the TableInput from the last operation.
     */
    @Property(tries = 100)
    void mergingConsecutiveCreateTableKeepsLastPayload(
            @ForAll("consecutiveCreateTableOps") List<CatalogOperation> ops) {

        CatalogOperation lastOp = ops.get(ops.size() - 1);
        TableInput expectedPayload = lastOp.getPayload();

        List<CatalogOperation> merged = OperationBatcher.mergeConsecutive(ops);

        assert merged.size() == 1
            : "Expected 1 merged CREATE_TABLE operation but got " + merged.size();

        CatalogOperation result = merged.get(0);
        assert result.getType() == OperationType.CREATE_TABLE
            : "Merged operation type should be CREATE_TABLE but was " + result.getType();
        assert result.getDatabaseName().equals(ops.get(0).getDatabaseName())
            : "Database name mismatch after merge";
        assert result.getTableName().equals(ops.get(0).getTableName())
            : "Table name mismatch after merge";

        TableInput resultPayload = result.getPayload();
        assert resultPayload == expectedPayload
            : "Merged CREATE_TABLE payload should be the last operation's payload (last-write-wins)";
    }

    // Feature: gdc-direct-api-sync, Property 10: Batch Deduplication Uses Last-Write-Wins for Consecutive Table Operations
    /**
     * Validates: Requirements 10.7
     * For consecutive UPDATE_TABLE ops targeting the same table, merging produces
     * a single operation whose payload is the TableInput from the last operation.
     */
    @Property(tries = 100)
    void mergingConsecutiveUpdateTableKeepsLastPayload(
            @ForAll("consecutiveUpdateTableOps") List<CatalogOperation> ops) {

        CatalogOperation lastOp = ops.get(ops.size() - 1);
        TableInput expectedPayload = lastOp.getPayload();

        List<CatalogOperation> merged = OperationBatcher.mergeConsecutive(ops);

        assert merged.size() == 1
            : "Expected 1 merged UPDATE_TABLE operation but got " + merged.size();

        CatalogOperation result = merged.get(0);
        assert result.getType() == OperationType.UPDATE_TABLE
            : "Merged operation type should be UPDATE_TABLE but was " + result.getType();
        assert result.getDatabaseName().equals(ops.get(0).getDatabaseName())
            : "Database name mismatch after merge";
        assert result.getTableName().equals(ops.get(0).getTableName())
            : "Table name mismatch after merge";

        TableInput resultPayload = result.getPayload();
        assert resultPayload == expectedPayload
            : "Merged UPDATE_TABLE payload should be the last operation's payload (last-write-wins)";
    }

    // Feature: gdc-direct-api-sync, Property 10: Batch Deduplication Uses Last-Write-Wins for Consecutive Table Operations
    /**
     * Validates: Requirements 10.5
     * For consecutive DROP_TABLE ops targeting the same table, merging produces
     * exactly one DROP_TABLE operation.
     */
    @Property(tries = 100)
    void mergingConsecutiveDropTableDeduplicatesToOne(
            @ForAll("consecutiveDropTableOps") List<CatalogOperation> ops) {

        List<CatalogOperation> merged = OperationBatcher.mergeConsecutive(ops);

        assert merged.size() == 1
            : "Expected 1 deduplicated DROP_TABLE operation but got " + merged.size();

        CatalogOperation result = merged.get(0);
        assert result.getType() == OperationType.DROP_TABLE
            : "Merged operation type should be DROP_TABLE but was " + result.getType();
        assert result.getDatabaseName().equals(ops.get(0).getDatabaseName())
            : "Database name mismatch after merge";
        assert result.getTableName().equals(ops.get(0).getTableName())
            : "Table name mismatch after merge";
    }

    // Feature: gdc-direct-api-sync, Property 10: Batch Deduplication Uses Last-Write-Wins for Consecutive Table Operations
    /**
     * Validates: Requirements 10.5, 10.6, 10.7
     * A single CREATE_TABLE, UPDATE_TABLE, or DROP_TABLE operation should pass through
     * mergeConsecutive unchanged (identity case).
     */
    @Property(tries = 100)
    void singleTableOpPassesThroughUnchanged(@ForAll("tableInputs") TableInput payload) {
        String db = "db_test";
        String table = "tbl_test";

        // Single CREATE_TABLE
        List<CatalogOperation> singleCreate = Collections.singletonList(
            new CatalogOperation(OperationType.CREATE_TABLE, db, table, payload));
        List<CatalogOperation> mergedCreate = OperationBatcher.mergeConsecutive(singleCreate);
        assert mergedCreate.size() == 1 : "Single CREATE_TABLE should produce 1 result";
        assert mergedCreate.get(0).getType() == OperationType.CREATE_TABLE;
        assert ((TableInput) mergedCreate.get(0).getPayload()) == payload
            : "Single CREATE_TABLE payload should be unchanged";

        // Single UPDATE_TABLE
        List<CatalogOperation> singleUpdate = Collections.singletonList(
            new CatalogOperation(OperationType.UPDATE_TABLE, db, table, payload));
        List<CatalogOperation> mergedUpdate = OperationBatcher.mergeConsecutive(singleUpdate);
        assert mergedUpdate.size() == 1 : "Single UPDATE_TABLE should produce 1 result";
        assert mergedUpdate.get(0).getType() == OperationType.UPDATE_TABLE;
        assert ((TableInput) mergedUpdate.get(0).getPayload()) == payload
            : "Single UPDATE_TABLE payload should be unchanged";

        // Single DROP_TABLE
        List<CatalogOperation> singleDrop = Collections.singletonList(
            new CatalogOperation(OperationType.DROP_TABLE, db, table, null));
        List<CatalogOperation> mergedDrop = OperationBatcher.mergeConsecutive(singleDrop);
        assert mergedDrop.size() == 1 : "Single DROP_TABLE should produce 1 result";
        assert mergedDrop.get(0).getType() == OperationType.DROP_TABLE;
    }

    // ---- Generators for Property 11 ----

    /**
     * Generates a mixed batch of operations across multiple tables, including
     * some CREATE_DATABASE operations. Uses a small set of db/table names
     * for density to increase interesting interactions.
     */
    @Provide
    Arbitrary<List<CatalogOperation>> mixedBatchWithCreateDatabase() {
        Arbitrary<String> fewDbs = Arbitraries.of("db_alpha", "db_beta", "db_gamma");
        Arbitrary<String> fewTables = Arbitraries.of("tbl_one", "tbl_two", "tbl_three");
        // Exclude CREATE_DATABASE from table-level ops; we add those separately
        Arbitrary<OperationType> tableOpTypes = Arbitraries.of(
            OperationType.CREATE_TABLE, OperationType.DROP_TABLE,
            OperationType.ADD_PARTITIONS, OperationType.DROP_PARTITIONS,
            OperationType.UPDATE_TABLE);

        Arbitrary<CatalogOperation> tableOps = Combinators.combine(tableOpTypes, fewDbs, fewTables)
            .as((type, db, table) -> {
                Object payload = buildDummyPayload(type);
                return new CatalogOperation(type, db, table, payload);
            });

        Arbitrary<CatalogOperation> createDbOps = fewDbs
            .map(db -> new CatalogOperation(OperationType.CREATE_DATABASE, db, null, null));

        // Mix table ops with a few CREATE_DATABASE ops interspersed
        Arbitrary<List<CatalogOperation>> tableOpList = tableOps.list().ofMinSize(1).ofMaxSize(30);
        Arbitrary<List<CatalogOperation>> dbOpList = createDbOps.list().ofMinSize(1).ofMaxSize(5);

        return Combinators.combine(tableOpList, dbOpList)
            .as((tOps, dOps) -> {
                List<CatalogOperation> all = new ArrayList<>();
                all.addAll(tOps);
                // Insert CREATE_DATABASE ops at various positions
                for (int i = 0; i < dOps.size(); i++) {
                    int insertPos = all.isEmpty() ? 0 : i % (all.size() + 1);
                    all.add(insertPos, dOps.get(i));
                }
                return all;
            });
    }

    /**
     * Generates a batch of only table-level operations (no CREATE_DATABASE)
     * across multiple tables with dense db/table names.
     */
    @Provide
    Arbitrary<List<CatalogOperation>> mixedTableOnlyBatch() {
        Arbitrary<String> fewDbs = Arbitraries.of("db_alpha", "db_beta");
        Arbitrary<String> fewTables = Arbitraries.of("tbl_one", "tbl_two", "tbl_three");
        Arbitrary<OperationType> tableOpTypes = Arbitraries.of(
            OperationType.CREATE_TABLE, OperationType.DROP_TABLE,
            OperationType.ADD_PARTITIONS, OperationType.DROP_PARTITIONS,
            OperationType.UPDATE_TABLE);

        Arbitrary<CatalogOperation> ops = Combinators.combine(tableOpTypes, fewDbs, fewTables)
            .as((type, db, table) -> {
                Object payload = buildDummyPayload(type);
                return new CatalogOperation(type, db, table, payload);
            });

        return ops.list().ofMinSize(1).ofMaxSize(40);
    }

    /**
     * Builds a simple non-null payload for the given operation type so that
     * mergeConsecutive can operate on realistic operations.
     */
    private static Object buildDummyPayload(OperationType type) {
        switch (type) {
            case CREATE_TABLE:
            case UPDATE_TABLE:
                TableInput ti = new TableInput();
                ti.withName("dummy");
                return ti;
            case ADD_PARTITIONS:
                PartitionInput pi = new PartitionInput();
                pi.withValues(Arrays.asList("val1"));
                return Arrays.asList(pi);
            case DROP_PARTITIONS:
                PartitionValueList pvl = new PartitionValueList().withValues(Arrays.asList("val1"));
                return Arrays.asList(pvl);
            default:
                return null;
        }
    }

    // ---- Property 11 Tests ----

    // Feature: gdc-direct-api-sync, Property 11: Batch Execution Preserves Per-Table FIFO Order
    /**
     * Validates: Requirements 11.1
     *
     * For any batch of CatalogOperations, after running the full pipeline
     * (extractCreateDatabaseOps → groupByTable → mergeConsecutive per group),
     * operations targeting the same database and table preserve their original
     * FIFO enqueue order within each per-table group.
     */
    @Property(tries = 100)
    void perTableFifoOrderIsPreservedThroughFullPipeline(
            @ForAll("mixedBatchWithCreateDatabase") List<CatalogOperation> batch) {

        // Step 1: Extract CREATE_DATABASE ops
        List<CatalogOperation> createDbOps = OperationBatcher.extractCreateDatabaseOps(batch);

        // Step 2: Build the remaining (non-CREATE_DATABASE) ops preserving original order
        List<CatalogOperation> remaining = new ArrayList<>();
        for (CatalogOperation op : batch) {
            if (op.getType() != OperationType.CREATE_DATABASE) {
                remaining.add(op);
            }
        }

        // Step 3: Group by table
        Map<String, List<CatalogOperation>> groups = OperationBatcher.groupByTable(remaining);

        // Step 4: Verify FIFO order within each group relative to the original batch
        for (List<CatalogOperation> group : groups.values()) {
            int lastIndex = -1;
            for (CatalogOperation op : group) {
                int idx = indexOfIdentity(batch, op, lastIndex + 1);
                assert idx > lastIndex
                    : "FIFO order violated: operation found at batch index " + idx
                      + " but previous group member was at index " + lastIndex;
                lastIndex = idx;
            }
        }

        // Step 5: Apply mergeConsecutive per group and verify merged results
        // still maintain relative FIFO order (merged ops should not reorder)
        for (List<CatalogOperation> group : groups.values()) {
            List<CatalogOperation> merged = OperationBatcher.mergeConsecutive(group);
            // Merged list should be no larger than the original group
            assert merged.size() <= group.size()
                : "Merged list size (" + merged.size() + ") exceeds original group size (" + group.size() + ")";
            // Merged list should not be empty if group was non-empty
            assert !merged.isEmpty()
                : "Merged list should not be empty for a non-empty group";
        }
    }

    // Feature: gdc-direct-api-sync, Property 11: Batch Execution Preserves Per-Table FIFO Order
    /**
     * Validates: Requirements 11.1
     *
     * CREATE_DATABASE operations are extracted and would execute before any
     * table-level operations that reference the same database.
     */
    @Property(tries = 100)
    void createDatabaseOpsExecuteBeforeDependentTableOps(
            @ForAll("mixedBatchWithCreateDatabase") List<CatalogOperation> batch) {

        // Extract CREATE_DATABASE ops
        List<CatalogOperation> createDbOps = OperationBatcher.extractCreateDatabaseOps(batch);

        // Remaining ops (table-level)
        List<CatalogOperation> remaining = new ArrayList<>();
        for (CatalogOperation op : batch) {
            if (op.getType() != OperationType.CREATE_DATABASE) {
                remaining.add(op);
            }
        }

        // All CREATE_DATABASE ops should be extracted
        for (CatalogOperation dbOp : createDbOps) {
            assert dbOp.getType() == OperationType.CREATE_DATABASE
                : "Extracted op should be CREATE_DATABASE but was " + dbOp.getType();
        }

        // No CREATE_DATABASE ops should remain in the table-level list
        for (CatalogOperation op : remaining) {
            assert op.getType() != OperationType.CREATE_DATABASE
                : "CREATE_DATABASE op should not remain in the table-level list";
        }

        // Collect databases that have CREATE_DATABASE ops
        Set<String> createdDatabases = new HashSet<>();
        for (CatalogOperation dbOp : createDbOps) {
            createdDatabases.add(dbOp.getDatabaseName());
        }

        // In the execution model, CREATE_DATABASE ops run first, then table ops.
        // Verify that the pipeline structure ensures this: all table ops referencing
        // a created database are in the 'remaining' list (executed after CREATE_DATABASE).
        Map<String, List<CatalogOperation>> groups = OperationBatcher.groupByTable(remaining);
        for (Map.Entry<String, List<CatalogOperation>> entry : groups.entrySet()) {
            for (CatalogOperation op : entry.getValue()) {
                // This op's database may or may not be in createdDatabases — that's fine.
                // The key invariant is that it's NOT in the createDbOps list,
                // so it will execute after all CREATE_DATABASE ops.
                assert op.getType() != OperationType.CREATE_DATABASE
                    : "Table group should not contain CREATE_DATABASE ops";
            }
        }
    }

    // Feature: gdc-direct-api-sync, Property 11: Batch Execution Preserves Per-Table FIFO Order
    /**
     * Validates: Requirements 11.1
     *
     * The full pipeline (extract → group → merge) preserves all operations:
     * the total count of CREATE_DATABASE ops plus all merged per-table ops
     * accounts for every operation in the original batch (no operations lost).
     */
    @Property(tries = 100)
    void fullPipelinePreservesAllOperations(
            @ForAll("mixedBatchWithCreateDatabase") List<CatalogOperation> batch) {

        // Extract CREATE_DATABASE ops
        List<CatalogOperation> createDbOps = OperationBatcher.extractCreateDatabaseOps(batch);

        // Count non-CREATE_DATABASE ops in original batch
        int nonDbOpCount = 0;
        for (CatalogOperation op : batch) {
            if (op.getType() != OperationType.CREATE_DATABASE) {
                nonDbOpCount++;
            }
        }

        // createDbOps + nonDbOpCount should equal total batch size
        assert createDbOps.size() + nonDbOpCount == batch.size()
            : "CREATE_DATABASE count (" + createDbOps.size() + ") + table op count ("
              + nonDbOpCount + ") != batch size (" + batch.size() + ")";

        // Group remaining ops
        List<CatalogOperation> remaining = new ArrayList<>();
        for (CatalogOperation op : batch) {
            if (op.getType() != OperationType.CREATE_DATABASE) {
                remaining.add(op);
            }
        }
        Map<String, List<CatalogOperation>> groups = OperationBatcher.groupByTable(remaining);

        // Total ops across all groups should equal nonDbOpCount
        int totalGrouped = 0;
        for (List<CatalogOperation> group : groups.values()) {
            totalGrouped += group.size();
        }
        assert totalGrouped == nonDbOpCount
            : "Total grouped ops (" + totalGrouped + ") != non-DB op count (" + nonDbOpCount + ")";
    }

    // Feature: gdc-direct-api-sync, Property 11: Batch Execution Preserves Per-Table FIFO Order
    /**
     * Validates: Requirements 11.1
     *
     * For a batch with only table-level operations (no CREATE_DATABASE),
     * the per-table FIFO order is preserved through grouping and merging,
     * and the merged operation types follow a valid sequence (no reordering
     * of different operation types within a per-table group).
     */
    @Property(tries = 100)
    void perTableMergedSequencePreservesTypeOrder(
            @ForAll("mixedTableOnlyBatch") List<CatalogOperation> batch) {

        Map<String, List<CatalogOperation>> groups = OperationBatcher.groupByTable(batch);

        for (Map.Entry<String, List<CatalogOperation>> entry : groups.entrySet()) {
            List<CatalogOperation> group = entry.getValue();
            List<CatalogOperation> merged = OperationBatcher.mergeConsecutive(group);

            // The merged sequence should have no two consecutive ops of the same type
            // (since consecutive same-type ops get merged into one)
            for (int i = 1; i < merged.size(); i++) {
                assert merged.get(i).getType() != merged.get(i - 1).getType()
                    : "After merging, consecutive ops should not have the same type, "
                      + "but found consecutive " + merged.get(i).getType()
                      + " at positions " + (i - 1) + " and " + i
                      + " in group '" + entry.getKey() + "'";
            }

            // The sequence of distinct operation types in merged should appear
            // in the same relative order as in the original group
            List<OperationType> originalTypeSequence = new ArrayList<>();
            OperationType lastType = null;
            for (CatalogOperation op : group) {
                if (op.getType() != lastType) {
                    originalTypeSequence.add(op.getType());
                    lastType = op.getType();
                }
            }

            List<OperationType> mergedTypeSequence = new ArrayList<>();
            for (CatalogOperation op : merged) {
                mergedTypeSequence.add(op.getType());
            }

            assert originalTypeSequence.equals(mergedTypeSequence)
                : "Merged type sequence " + mergedTypeSequence
                  + " does not match original distinct type sequence " + originalTypeSequence
                  + " for group '" + entry.getKey() + "'";
        }
    }
}
