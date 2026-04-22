package com.amazonaws.services.glue.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveGlueCatalogSyncAgent extends MetaStoreEventListener {
	private static final Logger LOG = LoggerFactory.getLogger(HiveGlueCatalogSyncAgent.class);
	private static final String GLUE_CATALOG_DROP_TABLE_IF_EXISTS = "glue.catalog.dropTableIfExists";
	private static final String GLUE_CATALOG_CREATE_MISSING_DB = "glue.catalog.createMissingDB";
	private static final String SUPPRESS_ALL_DROP_EVENTS = "glue.catalog.suppressAllDropEvents";
	private static final String GLUE_CATALOG_ID = "glue.catalog.id";
	private static final String GLUE_CATALOG_BATCH_WINDOW_SECONDS = "glue.catalog.batch.window.seconds";
	private static final String GLUE_CATALOG_SYNC_TABLE_STATISTICS = "glue.catalog.syncTableStatistics";
	private static final String GLUE_CATALOG_QUEUE_CAPACITY = "glue.catalog.queue.capacity";
	private static final String GLUE_CATALOG_TABLE_SYSTEM_OWNERSHIP_PROP = "table.system.ownership";
	private static final int DEFAULT_QUEUE_CAPACITY = 50000;

	/**
	 * Dedicated logger for rejected operations. Outputs machine-parseable TSV lines
	 * (database, table, operation_type, timestamp_ms) that downstream jobs can consume
	 * to manually sync objects that fell out of the queue.
	 */
	private static final Logger REJECTED_OPS_LOG =
			LoggerFactory.getLogger("com.amazonaws.services.glue.catalog.RejectedOperations");

	private Configuration config = null;
	private Thread queueProcessor;
	private volatile LinkedBlockingQueue<CatalogOperation> operationQueue;
	private boolean dropTableIfExists = false;
	private boolean createMissingDB = true;
	private int noEventSleepDuration;
	private int reconnectSleepDuration;
	private boolean suppressAllDropEvents = false;

	// GDC Direct API fields
	private AWSGlue glueClient;
	private String catalogId;
	private boolean syncTableStatistics = false;
	private int batchWindowSeconds = 60;
	private final Set<String> disallowedTables = ConcurrentHashMap.newKeySet();
	private MetricsCollector metricsCollector;

	/**
	 * Private class to cleanup the sync agent - to be used in a Runtime shutdown
	 * hook
	 *
	 */
	private final class SyncAgentShutdownRoutine implements Runnable {
		private GlueCatalogQueueProcessor p;
		private MetricsCollector metricsCollector;

		protected SyncAgentShutdownRoutine(GlueCatalogQueueProcessor queueProcessor, MetricsCollector metricsCollector) {
			this.p = queueProcessor;
			this.metricsCollector = metricsCollector;
		}

		public void run() {
			// stop the queue processing thread
			p.stop();
			// shutdown metrics collector (flushes remaining metrics)
			metricsCollector.shutdown();
		}
	}

	/**
	 * Queue processor that drains CatalogOperations from the queue in batch windows
	 * and executes them against the AWS Glue Data Catalog API directly.
	 *
	 * Transient error retries (429, 500, 503) are handled by the SDK's built-in
	 * retry policy configured in {@link GlueClientFactory}. This class only handles
	 * domain-specific exception recovery (AlreadyExistsException, EntityNotFoundException).
	 */
	final class GlueCatalogQueueProcessor implements Runnable {
		private final AWSGlue glueClient;
		private final CloudWatchLogsReporter cwlr;
		private final MetricsCollector metricsCollector;
		private final String catalogId;
		private final boolean dropTableIfExists;
		private final boolean createMissingDB;
		private final int batchWindowMs;
		private volatile boolean running = true;

		GlueCatalogQueueProcessor(AWSGlue glueClient, CloudWatchLogsReporter cwlr,
				MetricsCollector metricsCollector,
				String catalogId, boolean dropTableIfExists, boolean createMissingDB,
				int batchWindowMs) {
			this.glueClient = glueClient;
			this.cwlr = cwlr;
			this.metricsCollector = metricsCollector;
			this.catalogId = catalogId;
			this.dropTableIfExists = dropTableIfExists;
			this.createMissingDB = createMissingDB;
			this.batchWindowMs = batchWindowMs;
		}

		public void stop() {
			LOG.info("Stopping GlueCatalogQueueProcessor");
			this.running = false;
		}

		@Override
		public void run() {
			while (running || !operationQueue.isEmpty()) {
				try {
					Thread.sleep(batchWindowMs);
				} catch (InterruptedException e) {
					LOG.debug("Batch window sleep interrupted");
					Thread.currentThread().interrupt();
					if (!running) {
						break;
					}
				}

				// Record queue depth before draining
				int currentDepth = operationQueue.size();
				int totalCapacity = currentDepth + operationQueue.remainingCapacity();
				metricsCollector.recordQueueDepth(currentDepth);
				if (currentDepth > totalCapacity * 0.8) {
					LOG.warn("Queue depth {} exceeds 80% of capacity ({})", currentDepth, totalCapacity);
				}

				// Drain all operations from the queue
				List<CatalogOperation> batch = new ArrayList<>();
				CatalogOperation op;
				while ((op = operationQueue.poll()) != null) {
					batch.add(op);
				}

				if (!batch.isEmpty()) {
					metricsCollector.recordBatchSize(batch.size());
					long batchStart = System.currentTimeMillis();
					processBatch(batch);
					metricsCollector.recordBatchProcessingTimeMs(System.currentTimeMillis() - batchStart);
				}
			}
			LOG.info("GlueCatalogQueueProcessor stopped");
		}

		private void processBatch(List<CatalogOperation> batch) {
			// 1. Extract CREATE_DATABASE ops to execute first
			List<CatalogOperation> dbOps = OperationBatcher.extractCreateDatabaseOps(batch);

			// 2. Remove CREATE_DATABASE ops from the batch for table-level processing
			List<CatalogOperation> tableOps = new ArrayList<>();
			for (CatalogOperation o : batch) {
				if (o.getType() != CatalogOperation.OperationType.CREATE_DATABASE) {
					tableOps.add(o);
				}
			}

			// 3. Execute CREATE_DATABASE operations first
			for (CatalogOperation dbOp : dbOps) {
				executeOperation(dbOp);
			}

			// 4. Group by table, preserving FIFO order
			Map<String, List<CatalogOperation>> groups = OperationBatcher.groupByTable(tableOps);

			// 5. Merge consecutive operations within each group, then execute
			for (Map.Entry<String, List<CatalogOperation>> entry : groups.entrySet()) {
				List<CatalogOperation> merged = OperationBatcher.mergeConsecutive(entry.getValue());
				for (CatalogOperation mergedOp : merged) {
					executeOperation(mergedOp);
				}
			}
		}

		private void executeOperation(CatalogOperation op) {
			switch (op.getType()) {
				case CREATE_TABLE:
					executeCreateTable(op);
					break;
				case DROP_TABLE:
					executeDropTable(op);
					break;
				case ADD_PARTITIONS:
					executeAddPartitions(op);
					break;
				case DROP_PARTITIONS:
					executeDropPartitions(op);
					break;
				case UPDATE_TABLE:
					executeUpdateTable(op);
					break;
				case CREATE_DATABASE:
					executeCreateDatabase(op);
					break;
				default:
					LOG.warn("Unknown operation type: {}", op.getType());
			}
			// Record sync lag from enqueue to completion
			metricsCollector.recordSyncLagMs(System.currentTimeMillis() - op.getCreatedAtMillis());
		}

		private void executeCreateTable(CatalogOperation op) {
			TableInput tableInput = op.getPayload();
			CreateTableRequest request = new CreateTableRequest()
					.withDatabaseName(op.getDatabaseName())
					.withTableInput(tableInput);
			if (catalogId != null) {
				request.withCatalogId(catalogId);
			}

			try {
				glueClient.createTable(request);
				cwlr.sendToCWL("SUCCESS: CREATE_TABLE " + op.getFullTableName());
				LOG.info("Successfully created table {}", op.getFullTableName());
				metricsCollector.recordOperationSuccess(op.getType());
			} catch (AlreadyExistsException e) {
				if (dropTableIfExists) {
					LOG.info("Table {} already exists, dropping and retrying", op.getFullTableName());
					cwlr.sendToCWL("INFO: CREATE_TABLE " + op.getFullTableName()
							+ " already exists, dropping and retrying");
					try {
						DeleteTableRequest deleteRequest = new DeleteTableRequest()
								.withDatabaseName(op.getDatabaseName())
								.withName(op.getTableName());
						if (catalogId != null) {
							deleteRequest.withCatalogId(catalogId);
						}
						glueClient.deleteTable(deleteRequest);
						glueClient.createTable(request);
						cwlr.sendToCWL("SUCCESS: CREATE_TABLE " + op.getFullTableName()
								+ " (after drop+retry)");
						LOG.info("Successfully created table {} after drop+retry", op.getFullTableName());
						metricsCollector.recordOperationSuccess(op.getType());
					} catch (Exception retryEx) {
						cwlr.sendToCWL("ERROR: CREATE_TABLE " + op.getFullTableName()
								+ " drop+retry failed: " + retryEx.getMessage());
						LOG.error("Failed to drop+retry create table {}: {}",
								op.getFullTableName(), retryEx.getMessage());
						metricsCollector.recordOperationFailure(op.getType());
					}
				} else {
					addToBlacklist(op.getDatabaseName(), op.getTableName());
					cwlr.sendToCWL("DISALLOWED: CREATE_TABLE " + op.getFullTableName()
							+ " already exists and dropTableIfExists is disabled");
					LOG.warn("Table {} already exists and dropTableIfExists is disabled, blacklisting",
							op.getFullTableName());
					metricsCollector.recordOperationFailure(op.getType());
				}
			} catch (EntityNotFoundException e) {
				if (createMissingDB && e.getMessage() != null
						&& e.getMessage().toLowerCase().contains("database")) {
					LOG.info("Database {} not found for table {}, creating database and retrying",
							op.getDatabaseName(), op.getFullTableName());
					cwlr.sendToCWL("INFO: CREATE_TABLE " + op.getFullTableName()
							+ " database not found, creating database and retrying");
					try {
						DatabaseInput dbInput = new DatabaseInput()
								.withName(op.getDatabaseName());
						CreateDatabaseRequest dbRequest = new CreateDatabaseRequest()
								.withDatabaseInput(dbInput);
						if (catalogId != null) {
							dbRequest.withCatalogId(catalogId);
						}
						glueClient.createDatabase(dbRequest);
						glueClient.createTable(request);
						cwlr.sendToCWL("SUCCESS: CREATE_TABLE " + op.getFullTableName()
								+ " (after createDatabase+retry)");
						LOG.info("Successfully created table {} after creating database {}",
								op.getFullTableName(), op.getDatabaseName());
						metricsCollector.recordOperationSuccess(op.getType());
					} catch (Exception retryEx) {
						cwlr.sendToCWL("ERROR: CREATE_TABLE " + op.getFullTableName()
								+ " createDatabase+retry failed: " + retryEx.getMessage());
						LOG.error("Failed to createDatabase+retry for table {}: {}",
								op.getFullTableName(), retryEx.getMessage());
						metricsCollector.recordOperationFailure(op.getType());
					}
				} else {
					cwlr.sendToCWL("ERROR: CREATE_TABLE " + op.getFullTableName()
							+ " EntityNotFoundException: " + e.getMessage());
					LOG.error("EntityNotFoundException creating table {}: {}",
							op.getFullTableName(), e.getMessage());
					metricsCollector.recordOperationFailure(op.getType());
				}
			} catch (AmazonServiceException e) {
				cwlr.sendToCWL("ERROR: CREATE_TABLE " + op.getFullTableName()
						+ " failed: " + e.getMessage());
				LOG.error("Failed to create table {}: {}", op.getFullTableName(), e.getMessage());
				metricsCollector.recordOperationFailure(op.getType());
			}
		}

		private void executeDropTable(CatalogOperation op) {
			DeleteTableRequest request = new DeleteTableRequest()
					.withDatabaseName(op.getDatabaseName())
					.withName(op.getTableName());
			if (catalogId != null) {
				request.withCatalogId(catalogId);
			}

			try {
				glueClient.deleteTable(request);
				cwlr.sendToCWL("SUCCESS: DROP_TABLE " + op.getFullTableName());
				LOG.info("Successfully dropped table {}", op.getFullTableName());
				metricsCollector.recordOperationSuccess(op.getType());
			} catch (EntityNotFoundException e) {
				cwlr.sendToCWL("INFO: DROP_TABLE " + op.getFullTableName()
						+ " not found in GDC, skipping");
				LOG.warn("Table {} not found in GDC during drop, skipping", op.getFullTableName());
				metricsCollector.recordOperationSuccess(op.getType());
			} catch (AmazonServiceException e) {
				cwlr.sendToCWL("ERROR: DROP_TABLE " + op.getFullTableName()
						+ " failed: " + e.getMessage());
				LOG.error("Failed to drop table {}: {}", op.getFullTableName(), e.getMessage());
				metricsCollector.recordOperationFailure(op.getType());
			}
		}

		@SuppressWarnings("unchecked")
		private void executeAddPartitions(CatalogOperation op) {
			List<PartitionInput> partitions = op.getPayload();
			BatchCreatePartitionRequest request = new BatchCreatePartitionRequest()
					.withDatabaseName(op.getDatabaseName())
					.withTableName(op.getTableName())
					.withPartitionInputList(partitions);
			if (catalogId != null) {
				request.withCatalogId(catalogId);
			}

			try {
				glueClient.batchCreatePartition(request);
				cwlr.sendToCWL("SUCCESS: ADD_PARTITIONS " + op.getFullTableName()
						+ " (" + partitions.size() + " partitions)");
				LOG.info("Successfully added {} partitions to {}", partitions.size(), op.getFullTableName());
				metricsCollector.recordOperationSuccess(op.getType());
			} catch (EntityNotFoundException e) {
				cwlr.sendToCWL("INFO: ADD_PARTITIONS " + op.getFullTableName()
						+ " table not found in GDC, skipping");
				LOG.warn("Table {} not found in GDC during addPartitions, skipping", op.getFullTableName());
				metricsCollector.recordOperationFailure(op.getType());
			} catch (AmazonServiceException e) {
				cwlr.sendToCWL("ERROR: ADD_PARTITIONS " + op.getFullTableName()
						+ " failed: " + e.getMessage());
				LOG.error("Failed to add partitions to {}: {}", op.getFullTableName(), e.getMessage());
				metricsCollector.recordOperationFailure(op.getType());
			}
		}

		@SuppressWarnings("unchecked")
		private void executeDropPartitions(CatalogOperation op) {
			List<PartitionValueList> partitionValues = op.getPayload();
			BatchDeletePartitionRequest request = new BatchDeletePartitionRequest()
					.withDatabaseName(op.getDatabaseName())
					.withTableName(op.getTableName())
					.withPartitionsToDelete(partitionValues);
			if (catalogId != null) {
				request.withCatalogId(catalogId);
			}

			try {
				glueClient.batchDeletePartition(request);
				cwlr.sendToCWL("SUCCESS: DROP_PARTITIONS " + op.getFullTableName()
						+ " (" + partitionValues.size() + " partitions)");
				LOG.info("Successfully dropped {} partitions from {}", partitionValues.size(),
						op.getFullTableName());
				metricsCollector.recordOperationSuccess(op.getType());
			} catch (EntityNotFoundException e) {
				cwlr.sendToCWL("INFO: DROP_PARTITIONS " + op.getFullTableName()
						+ " table not found in GDC, skipping");
				LOG.warn("Table {} not found in GDC during dropPartitions, skipping", op.getFullTableName());
				metricsCollector.recordOperationSuccess(op.getType());
			} catch (AmazonServiceException e) {
				cwlr.sendToCWL("ERROR: DROP_PARTITIONS " + op.getFullTableName()
						+ " failed: " + e.getMessage());
				LOG.error("Failed to drop partitions from {}: {}", op.getFullTableName(), e.getMessage());
				metricsCollector.recordOperationFailure(op.getType());
			}
		}

		private void executeUpdateTable(CatalogOperation op) {
			TableInput tableInput = op.getPayload();
			UpdateTableRequest request = new UpdateTableRequest()
					.withDatabaseName(op.getDatabaseName())
					.withTableInput(tableInput);
			if (catalogId != null) {
				request.withCatalogId(catalogId);
			}

			try {
				glueClient.updateTable(request);
				cwlr.sendToCWL("SUCCESS: UPDATE_TABLE " + op.getFullTableName());
				LOG.info("Successfully updated table {}", op.getFullTableName());
				metricsCollector.recordOperationSuccess(op.getType());
			} catch (AmazonServiceException e) {
				cwlr.sendToCWL("ERROR: UPDATE_TABLE " + op.getFullTableName()
						+ " failed: " + e.getMessage());
				LOG.error("Failed to update table {}: {}", op.getFullTableName(), e.getMessage());
				metricsCollector.recordOperationFailure(op.getType());
			}
		}

		private void executeCreateDatabase(CatalogOperation op) {
			DatabaseInput dbInput = op.getPayload();
			CreateDatabaseRequest request = new CreateDatabaseRequest()
					.withDatabaseInput(dbInput);
			if (catalogId != null) {
				request.withCatalogId(catalogId);
			}

			try {
				glueClient.createDatabase(request);
				cwlr.sendToCWL("SUCCESS: CREATE_DATABASE " + op.getDatabaseName());
				LOG.info("Successfully created database {}", op.getDatabaseName());
				metricsCollector.recordOperationSuccess(op.getType());
			} catch (AlreadyExistsException e) {
				cwlr.sendToCWL("INFO: CREATE_DATABASE " + op.getDatabaseName()
						+ " already exists, skipping");
				LOG.info("Database {} already exists, skipping creation", op.getDatabaseName());
				metricsCollector.recordOperationSuccess(op.getType());
			} catch (AmazonServiceException e) {
				cwlr.sendToCWL("ERROR: CREATE_DATABASE " + op.getDatabaseName()
						+ " failed: " + e.getMessage());
				LOG.error("Failed to create database {}: {}", op.getDatabaseName(), e.getMessage());
				metricsCollector.recordOperationFailure(op.getType());
			}
		}
	}

	/** Dummy constructor for unit tests */
	public HiveGlueCatalogSyncAgent() throws Exception {
		super(null);
	}

	public HiveGlueCatalogSyncAgent(final Configuration conf) throws Exception {
		super(conf);
		this.config = conf;

		String noopSleepDuration = this.config.get("no-event-sleep-duration");
		if (noopSleepDuration == null) {
			this.noEventSleepDuration = 1000;
		} else {
			this.noEventSleepDuration = new Integer(noopSleepDuration).intValue();
		}

		String reconnectSleepDuration = conf.get("reconnect-failed-sleep-duration");
		if (reconnectSleepDuration == null) {
			this.reconnectSleepDuration = 1000;
		} else {
			this.reconnectSleepDuration = new Integer(noopSleepDuration).intValue();
		}

		dropTableIfExists = config.getBoolean(GLUE_CATALOG_DROP_TABLE_IF_EXISTS, false);
		createMissingDB = config.getBoolean(GLUE_CATALOG_CREATE_MISSING_DB, true);
		suppressAllDropEvents = config.getBoolean(SUPPRESS_ALL_DROP_EVENTS, false);

		// Read GDC Direct API config properties
		this.catalogId = config.get(GLUE_CATALOG_ID, null);
		this.syncTableStatistics = config.getBoolean(GLUE_CATALOG_SYNC_TABLE_STATISTICS, false);
		this.batchWindowSeconds = config.getInt(GLUE_CATALOG_BATCH_WINDOW_SECONDS, 60);

		// Create Glue client (retry policy for transient errors is configured in GlueClientFactory)
		this.glueClient = GlueClientFactory.createClient(config);

		operationQueue = new LinkedBlockingQueue<>(
				config.getInt(GLUE_CATALOG_QUEUE_CAPACITY, DEFAULT_QUEUE_CAPACITY));

		// start the queue processor thread
		CloudWatchLogsReporter cwlr = new CloudWatchLogsReporter(this.config);
		this.metricsCollector = MetricsCollectorFactory.create(config);
		GlueCatalogQueueProcessor glueCatalogQueueProcessor = new GlueCatalogQueueProcessor(
				this.glueClient, cwlr, metricsCollector, this.catalogId, this.dropTableIfExists, this.createMissingDB,
				this.batchWindowSeconds * 1000);
		queueProcessor = new Thread(glueCatalogQueueProcessor, "GlueCatalogSyncThread");
		queueProcessor.start();

		// add a shutdown hook to close the connections
		Runtime.getRuntime()
				.addShutdownHook(new Thread(new SyncAgentShutdownRoutine(glueCatalogQueueProcessor, metricsCollector), "Shutdown-thread"));

		LOG.info(String.format("%s online, using GDC Direct API (region: %s, catalogId: %s, queueCapacity: %d)",
				this.getClass().getCanonicalName(),
				config.get(GlueClientFactory.GLUE_CATALOG_REGION, GlueClientFactory.DEFAULT_REGION),
				this.catalogId != null ? this.catalogId : "default",
				operationQueue.remainingCapacity()));
	}

	/**
	 * Checks if a table is eligible for sync to GDC.
	 * A table is eligible if its location (after s3a/s3n translation) starts with "s3"
	 * and it does not use a custom storage handler.
	 */
	boolean isSyncEligible(Table table) {
		if (table.getSd() == null || table.getSd().getLocation() == null) {
			return false;
		}
		// Check for custom storage handler — tables with storage handlers (e.g., HBase) are not supported
		if (table.getParameters() != null && table.getParameters().containsKey("storage_handler")) {
			LOG.debug("Table {} has a custom storage handler, skipping sync", getFqtn(table));
			return false;
		}
		// Check S3 location after translation
		String translatedLocation = TableInputBuilder.translateLocation(table.getSd().getLocation());
		if (!translatedLocation.startsWith("s3")) {
			LOG.debug("Table {} location is not S3-based ({}), skipping sync", getFqtn(table), translatedLocation);
			return false;
		}
		return true;
	}

	/**
	 * Checks if the table ownership allows sync.
	 * Tables with table.system.ownership set to "gdc" are owned by GDC and should not be synced.
	 * Tables with ownership "hms" or absent are eligible for sync.
	 */
	boolean isTableOwnershipValid(Table table) {
		if (table.getParameters() == null) {
			return true;
		}
		String ownership = table.getParameters().get(GLUE_CATALOG_TABLE_SYSTEM_OWNERSHIP_PROP);
		if ("gdc".equals(ownership)) {
			LOG.debug("Table {} is owned by GDC, skipping sync", getFqtn(table));
			return false;
		}
		return true;
	}

	/**
	 * Checks if a table is disallowed from sync operations.
	 */
	boolean isDisallowed(String dbName, String tableName) {
		String key = dbName + "." + tableName;
		if (disallowedTables.contains(key)) {
			LOG.debug("Table {} is disallowed, skipping sync", key);
			return true;
		}
		return false;
	}

	/**
	 * Adds a table to the blacklist, preventing further sync operations.
	 */
	void addToBlacklist(String dbName, String tableName) {
		String key = dbName + "." + tableName;
		disallowedTables.add(key);
		LOG.info("Table {} added to blacklist", key);
	}

	/**
	 * Enqueues a CatalogOperation for processing by the queue processor.
	 * Uses non-blocking offer() — if the bounded queue is full, the operation
	 * is rejected and logged to the dedicated rejected-operations logger in
	 * machine-parseable TSV format for downstream manual sync.
	 */
	boolean addToQueue(CatalogOperation operation) {
		try {
			if (operationQueue.offer(operation)) {
				return true;
			}
			// Queue is full — reject with parseable output
			metricsCollector.recordQueueRejection(operation.getType());
			LOG.error("Queue full — rejected operation {} for {}",
					operation.getType(), operation.getFullTableName());
			REJECTED_OPS_LOG.error("{}\t{}\t{}\t{}",
					operation.getDatabaseName(),
					operation.getTableName() != null ? operation.getTableName() : "",
					operation.getType(),
					System.currentTimeMillis());
			return false;
		} catch (Exception e) {
			LOG.error("Failed to enqueue operation {} for {}: {}",
					operation.getType(), operation.getFullTableName(), e.getMessage());
			return false;
		}
	}

	/** Return the fully qualified table name for a table */
	private static String getFqtn(Table table) {
		return table.getDbName() + "." + table.getTableName();
	}

	/**
	 * Handler for a Drop Table event
	 */
	public void onDropTable(DropTableEvent tableEvent) throws MetaException {
		super.onDropTable(tableEvent);

		if (suppressAllDropEvents) {
			LOG.debug("Ignoring DropTable event as {} set to True", SUPPRESS_ALL_DROP_EVENTS);
			return;
		}

		Table table = tableEvent.getTable();
		String dbName = table.getDbName();
		String tableName = table.getTableName();

		if (!isSyncEligible(table)) {
			return;
		}
		if (!isTableOwnershipValid(table)) {
			return;
		}
		if (isDisallowed(dbName, tableName)) {
			return;
		}

		CatalogOperation operation = new CatalogOperation(
				CatalogOperation.OperationType.DROP_TABLE, dbName, tableName, null);

		if (!addToQueue(operation)) {
			LOG.error("Failed to add the DropTable event to the processing queue");
		} else {
			LOG.debug("Requested Drop of table: {}", tableName);
		}
	}

	/**
	 * Handler for a CreateTable Event
	 */
	public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
		super.onCreateTable(tableEvent);

		Table table = tableEvent.getTable();
		String dbName = table.getDbName();
		String tableName = table.getTableName();

		if (!isSyncEligible(table)) {
			return;
		}
		if (!isTableOwnershipValid(table)) {
			return;
		}
		if (isDisallowed(dbName, tableName)) {
			return;
		}

		TableInput tableInput = TableInputBuilder.buildTableInput(table, syncTableStatistics);
		CatalogOperation operation = new CatalogOperation(
				CatalogOperation.OperationType.CREATE_TABLE, dbName, tableName, tableInput);

		if (!addToQueue(operation)) {
			LOG.error("Failed to add the CreateTable event to the processing queue");
		} else {
			LOG.debug("Requested replication of {} to AWS Glue Catalog.", tableName);
		}
	}

	/**
	 * Handler for an AddPartition event
	 */
	public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
		super.onAddPartition(partitionEvent);

		if (!partitionEvent.getStatus()) {
			return;
		}

		Table table = partitionEvent.getTable();
		String dbName = table.getDbName();
		String tableName = table.getTableName();

		if (!isSyncEligible(table)) {
			return;
		}
		if (!isTableOwnershipValid(table)) {
			return;
		}
		if (isDisallowed(dbName, tableName)) {
			return;
		}

		List<PartitionInput> partitionInputs = new ArrayList<>();
		partitionEvent.getPartitionIterator().forEachRemaining(p -> {
			if (p.getSd() != null && p.getSd().getLocation() != null) {
				String translatedLocation = TableInputBuilder.translateLocation(p.getSd().getLocation());
				if (translatedLocation.startsWith("s3")) {
					partitionInputs.add(TableInputBuilder.buildPartitionInput(p));
				} else {
					LOG.debug("Not adding partition as it is not S3 based (location {})", p.getSd().getLocation());
				}
			}
		});

		if (!partitionInputs.isEmpty()) {
			CatalogOperation operation = new CatalogOperation(
					CatalogOperation.OperationType.ADD_PARTITIONS, dbName, tableName, partitionInputs);

			if (!addToQueue(operation)) {
				LOG.error("Failed to add the AddPartition event to the processing queue");
			}
		}
	}

	/**
	 * Handler to deal with partition drop events. Receives a single partition drop
	 * event and drops all partitions included in the event.
	 */
	public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
		super.onDropPartition(partitionEvent);

		if (suppressAllDropEvents) {
			LOG.debug("Ignoring DropPartition event as {} set to True", SUPPRESS_ALL_DROP_EVENTS);
			return;
		}

		if (!partitionEvent.getStatus()) {
			return;
		}

		Table table = partitionEvent.getTable();
		String dbName = table.getDbName();
		String tableName = table.getTableName();

		if (!isSyncEligible(table)) {
			return;
		}
		if (!isTableOwnershipValid(table)) {
			return;
		}
		if (isDisallowed(dbName, tableName)) {
			return;
		}

		List<PartitionValueList> partitionValueLists = new ArrayList<>();
		partitionEvent.getPartitionIterator().forEachRemaining(p -> {
			if (p.getSd() != null && p.getSd().getLocation() != null) {
				String translatedLocation = TableInputBuilder.translateLocation(p.getSd().getLocation());
				if (translatedLocation.startsWith("s3")) {
					PartitionValueList pvl = new PartitionValueList()
							.withValues(new ArrayList<>(p.getValues()));
					partitionValueLists.add(pvl);
				} else {
					LOG.debug("Not dropping partition as it is not S3 based (location {})", p.getSd().getLocation());
				}
			}
		});

		if (!partitionValueLists.isEmpty()) {
			CatalogOperation operation = new CatalogOperation(
					CatalogOperation.OperationType.DROP_PARTITIONS, dbName, tableName, partitionValueLists);

			if (!addToQueue(operation)) {
				LOG.error("Failed to add the DropPartition event to the processing queue");
			}
		}
	}

	@Override
	public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
		super.onAlterTable(tableEvent);

		final Table newTable = tableEvent.getNewTable();
		String dbName = newTable.getDbName();
		String tableName = newTable.getTableName();

		if (!isSyncEligible(newTable)) {
			LOG.debug("[AlterTableEvent] Ignoring AlterTable event for Table {} as it is not sync eligible", tableName);
			return;
		}
		if (!isTableOwnershipValid(newTable)) {
			return;
		}
		if (isDisallowed(dbName, tableName)) {
			return;
		}

		TableInput tableInput = TableInputBuilder.buildTableInput(newTable, syncTableStatistics);
		CatalogOperation operation = new CatalogOperation(
				CatalogOperation.OperationType.UPDATE_TABLE, dbName, tableName, tableInput);

		if (!addToQueue(operation)) {
			LOG.error("[AlterTableEvent] Failed to add the UPDATE_TABLE to the processing queue for table: {}", tableName);
		} else {
			LOG.debug("[AlterTableEvent] Requested UPDATE_TABLE for table: {}", tableName);
		}
	}
}
