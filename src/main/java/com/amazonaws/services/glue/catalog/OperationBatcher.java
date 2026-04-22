package com.amazonaws.services.glue.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;

import com.amazonaws.services.glue.catalog.CatalogOperation.OperationType;

/**
 * Groups, merges, and orders catalog operations for efficient batch execution.
 *
 * <p>Processing steps:
 * <ol>
 *   <li>Extract CREATE_DATABASE operations (executed first)</li>
 *   <li>Group remaining operations by (databaseName, tableName) preserving FIFO order</li>
 *   <li>Merge consecutive same-type operations within each per-table group</li>
 * </ol>
 */
public class OperationBatcher {

    private OperationBatcher() {
        // Utility class — not instantiable
    }

    /**
     * Groups operations by their full table name (databaseName.tableName),
     * preserving FIFO insertion order across groups and within each group.
     *
     * @param ops the list of catalog operations to group
     * @return a LinkedHashMap keyed by "databaseName.tableName" with FIFO-ordered operation lists
     */
    public static Map<String, List<CatalogOperation>> groupByTable(List<CatalogOperation> ops) {
        Map<String, List<CatalogOperation>> groups = new LinkedHashMap<>();
        for (CatalogOperation op : ops) {
            String key = op.getFullTableName();
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(op);
        }
        return groups;
    }

    /**
     * Merges consecutive same-type operations within a per-table group.
     *
     * <p>Merge rules:
     * <ul>
     *   <li>Consecutive ADD_PARTITIONS: concatenate partition lists into one operation</li>
     *   <li>Consecutive DROP_PARTITIONS: concatenate partition value lists into one operation</li>
     *   <li>Consecutive CREATE_TABLE: last-write-wins (keep only the last)</li>
     *   <li>Consecutive UPDATE_TABLE: last-write-wins (keep only the last)</li>
     *   <li>Consecutive DROP_TABLE: deduplicate to a single operation</li>
     * </ul>
     *
     * <p>Non-consecutive operations of the same type are NOT merged. This preserves
     * semantic ordering (e.g., DROP_TABLE followed by CREATE_TABLE must not be reordered).
     *
     * @param ops the per-table operation list (all ops share the same databaseName/tableName)
     * @return a new list with consecutive same-type operations merged
     */
    @SuppressWarnings("unchecked")
    public static List<CatalogOperation> mergeConsecutive(List<CatalogOperation> ops) {
        if (ops == null || ops.isEmpty()) {
            return new ArrayList<>();
        }

        List<CatalogOperation> merged = new ArrayList<>();
        int i = 0;

        while (i < ops.size()) {
            CatalogOperation current = ops.get(i);
            OperationType type = current.getType();

            // Find the end of the consecutive run of the same type
            int runEnd = i + 1;
            while (runEnd < ops.size() && ops.get(runEnd).getType() == type) {
                runEnd++;
            }

            if (runEnd - i == 1) {
                // Single operation, no merging needed
                merged.add(current);
            } else {
                // Multiple consecutive operations of the same type — merge them
                List<CatalogOperation> run = ops.subList(i, runEnd);
                merged.add(mergeRun(run, type));
            }

            i = runEnd;
        }

        return merged;
    }

    /**
     * Extracts CREATE_DATABASE operations from the list, returning them separately
     * so they can be executed before any table-level operations.
     *
     * @param ops the full list of catalog operations
     * @return a list containing only CREATE_DATABASE operations, in their original order
     */
    public static List<CatalogOperation> extractCreateDatabaseOps(List<CatalogOperation> ops) {
        List<CatalogOperation> dbOps = new ArrayList<>();
        for (CatalogOperation op : ops) {
            if (op.getType() == OperationType.CREATE_DATABASE) {
                dbOps.add(op);
            }
        }
        return dbOps;
    }

    /**
     * Merges a run of consecutive operations of the same type into a single operation.
     */
    @SuppressWarnings("unchecked")
    private static CatalogOperation mergeRun(List<CatalogOperation> run, OperationType type) {
        // Use the last operation as the base for metadata (databaseName, tableName)
        CatalogOperation last = run.get(run.size() - 1);

        switch (type) {
            case ADD_PARTITIONS:
                return mergeAddPartitions(run, last);
            case DROP_PARTITIONS:
                return mergeDropPartitions(run, last);
            case CREATE_TABLE:
            case UPDATE_TABLE:
                // Last-write-wins: return only the last operation
                return last;
            case DROP_TABLE:
                // Deduplicate: return only the first (they're all equivalent)
                return run.get(0);
            default:
                // For any other type (e.g., CREATE_DATABASE), just keep the last
                return last;
        }
    }

    /**
     * Merges consecutive ADD_PARTITIONS operations by concatenating their partition lists.
     */
    @SuppressWarnings("unchecked")
    private static CatalogOperation mergeAddPartitions(List<CatalogOperation> run, CatalogOperation last) {
        List<PartitionInput> allPartitions = new ArrayList<>();
        for (CatalogOperation op : run) {
            List<PartitionInput> partitions = op.getPayload();
            if (partitions != null) {
                allPartitions.addAll(partitions);
            }
        }
        return new CatalogOperation(
            OperationType.ADD_PARTITIONS,
            last.getDatabaseName(),
            last.getTableName(),
            allPartitions
        );
    }

    /**
     * Merges consecutive DROP_PARTITIONS operations by concatenating their partition value lists.
     */
    @SuppressWarnings("unchecked")
    private static CatalogOperation mergeDropPartitions(List<CatalogOperation> run, CatalogOperation last) {
        List<PartitionValueList> allValues = new ArrayList<>();
        for (CatalogOperation op : run) {
            List<PartitionValueList> values = op.getPayload();
            if (values != null) {
                allValues.addAll(values);
            }
        }
        return new CatalogOperation(
            OperationType.DROP_PARTITIONS,
            last.getDatabaseName(),
            last.getTableName(),
            allValues
        );
    }
}
