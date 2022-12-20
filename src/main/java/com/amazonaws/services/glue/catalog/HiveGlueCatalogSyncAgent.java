package com.amazonaws.services.glue.catalog;

import static com.amazonaws.services.glue.catalog.HiveUtils.getColumnNames;
import static com.amazonaws.services.glue.catalog.HiveUtils.translateLocationToS3Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import org.apache.commons.lang3.StringUtils;
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
	private static final String GLUE_CATALOG_USER_KEY = "glue.catalog.user.key";
	private final String ATHENA_JDBC_URL = "glue.catalog.athena.jdbc.url";
	private static final String GLUE_CATALOG_USER_SECRET = "glue.catalog.user.secret";
	private static final String GLUE_CATALOG_S3_STAGING_DIR = "glue.catalog.athena.s3.staging.dir";
	private static final String SUPPRESS_ALL_DROP_EVENTS = "glue.catalog.athena.suppressAllDropEvents";
	private static final String DEFAULT_ATHENA_CONNECTION_URL = "jdbc:awsathena://athena.us-east-1.amazonaws.com:443";

	private Configuration config = null;
	private Properties info;
	private String athenaURL;
	private Thread queueProcessor;
	private Connection athenaConnection;
	private volatile ConcurrentLinkedQueue<String> ddlQueue;
	private final String EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
	private boolean dropTableIfExists = false;
	private boolean createMissingDB = true;
	private int noEventSleepDuration;
	private int reconnectSleepDuration;
	private boolean suppressAllDropEvents = false;

	private final List<String> quotedTypes = new ArrayList<String>() {
		{
			add("string");
			add("date");
		}
	};

	/**
	 * Private class to cleanup the sync agent - to be used in a Runtime shutdown
	 * hook
	 *
	 * @author meyersi
	 */
	private final class SyncAgentShutdownRoutine implements Runnable {
		private AthenaQueueProcessor p;

		protected SyncAgentShutdownRoutine(AthenaQueueProcessor queueProcessor) {

			this.p = queueProcessor;
		}

		public void run() {
			// stop the queue processing thread
			p.stop();
		}
	}

	/**
	 * Private class which processes the ddl queue and pushes the ddl through
	 * Athena. If the Athena connection is broken, try and reconnect, and if not
	 * then back off for a period of time and hope that the connection is fixed
	 *
	 * @author meyersi
	 */
	private final class AthenaQueueProcessor implements Runnable {
		private boolean run = true;
		private CloudWatchLogsReporter cwlr;
		private final Pattern PATTERN = Pattern.compile("(?i)CREATE EXTERNAL TABLE (.*).");

		public AthenaQueueProcessor(Configuration config) {
			super();
			this.cwlr = new CloudWatchLogsReporter(config);
		}

		/**
		 * Method to send a shutdown message to the queue processor
		 */
		public void stop() {
			LOG.info(String.format("Stopping %s", this.getClass().getCanonicalName()));
			try {
				athenaConnection.close();
			} catch (SQLException e) {
				LOG.error(e.getMessage());
			}
			this.run = false;
		}

		public void run() {
			// run forever or until stop is called, and continue running until the queue is
			// empty when stop is called
			while (true) {
				if (!ddlQueue.isEmpty()) {
					String query = ddlQueue.poll();

					LOG.info("Working on " + query);
					// Exception logic: if it's a network issue keep retrying. Anything else log to
					// CWL and move on.
					boolean completed = false;
					while (!completed) {
						try {
							Statement athenaStmt = athenaConnection.createStatement();
							cwlr.sendToCWL("Trying to execute: " + query);
							athenaStmt.execute(query);
							athenaStmt.close();
							completed = true;
						} catch (SQLException e) {
							if (e instanceof SQLRecoverableException || e instanceof SQLTimeoutException) {
								try {
									configureAthenaConnection();
								} catch (SQLException e1) {
									// this will probably be because we can't open the connection
									try {
										Thread.sleep(reconnectSleepDuration);
									} catch (InterruptedException e2) {
										e2.printStackTrace();
									}
								}
							} else {
								// Athena's JDBC Driver just throws a generic SQLException
								// Only way to identify exception type is through string parsing :O=
								if (e.getMessage().contains("AlreadyExistsException") && dropTableIfExists) {
									Matcher matcher = PATTERN.matcher(query);
									matcher.find();
									String tableName = matcher.group(1);
									try {
										cwlr.sendToCWL("Dropping table " + tableName);
										Statement athenaStmt = athenaConnection.createStatement();
										athenaStmt.execute("drop table " + tableName);
										cwlr.sendToCWL("Creating table " + tableName + " after dropping ");
										athenaStmt.execute(query);
										athenaStmt.close();
									} catch (Exception e2) {
										cwlr.sendToCWL("Unable to drop and recreate  " + tableName);
										cwlr.sendToCWL("ERROR: " + e.getMessage());
									}
								} else if (e.getMessage().contains("Database does not exist:") && createMissingDB) {
									try {
										String dbName = e.getMessage().split(":")[3].trim();
										cwlr.sendToCWL("Trying to create database  " + dbName);
										Statement athenaStmt = athenaConnection.createStatement();
										athenaStmt.execute("Create database if not exists " + dbName);
										cwlr.sendToCWL("Retrying table creation:" + query);
										athenaStmt.execute(query);
										athenaStmt.close();
									} catch (Throwable e2) {
										LOG.info("ERROR: " + e.getMessage());
										LOG.info("DB doesn't exist for: " + query);
									}
								} else {
									LOG.info("Unable to complete query: " + query);
									cwlr.sendToCWL("ERROR: " + e.getMessage());
								}
								completed = true;
							}
						}
					}

				} else {
					// put the thread to sleep for a configured duration
					try {
						LOG.debug(String.format("DDL Queue is empty. Sleeping for %s, queue state is %s",
								noEventSleepDuration, ddlQueue.size()));
						Thread.sleep(noEventSleepDuration);
					} catch (InterruptedException e) {
						LOG.error(e.getMessage());
					}
				}
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

		this.info = new Properties();
		this.info.put("log_path", "/tmp/jdbc.log");
		this.info.put("log_level", "ERROR");
		this.info.put("s3_staging_dir", config.get(GLUE_CATALOG_S3_STAGING_DIR));

		dropTableIfExists = config.getBoolean(GLUE_CATALOG_DROP_TABLE_IF_EXISTS, false);
		createMissingDB = config.getBoolean(GLUE_CATALOG_CREATE_MISSING_DB, true);
		suppressAllDropEvents = config.getBoolean(SUPPRESS_ALL_DROP_EVENTS, false);
		this.athenaURL = conf.get(ATHENA_JDBC_URL, DEFAULT_ATHENA_CONNECTION_URL);

		if (config.get(GLUE_CATALOG_USER_KEY) != null) {
			info.put("user", config.get(GLUE_CATALOG_USER_KEY));
			info.put("password", config.get(GLUE_CATALOG_USER_SECRET));
		} else {
			this.info.put("aws_credentials_provider_class",
					com.amazonaws.auth.InstanceProfileCredentialsProvider.class.getName());
		}

		ddlQueue = new ConcurrentLinkedQueue<>();

		configureAthenaConnection();

		// start the queue processor thread
		AthenaQueueProcessor athenaQueueProcessor = new AthenaQueueProcessor(this.config);
		queueProcessor = new Thread(athenaQueueProcessor, "GlueSyncThread");
		queueProcessor.start();

		// add a shutdown hook to close the connections
		Runtime.getRuntime()
				.addShutdownHook(new Thread(new SyncAgentShutdownRoutine(athenaQueueProcessor), "Shutdown-thread"));

		LOG.info(String.format("%s online, connected to %s", this.getClass().getCanonicalName(), this.athenaURL));
	}

	private final void configureAthenaConnection() throws SQLException, SQLTimeoutException {
		LOG.info(String.format("Connecting to Amazon Athena using endpoint %s", this.athenaURL));
		athenaConnection = DriverManager.getConnection(this.athenaURL, this.info);
	}

	private boolean addToAthenaQueue(String query) {
		try {
			ddlQueue.add(query);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			return false;
		}
		return true;
	}

	/** Return the fully qualified table name for a table */
	private static String getFqtn(Table table) {
		return table.getDbName() + "." + table.getTableName();
	}

	/**
	 * function to extract and return the partition specification for a given spec,
	 * in format of (name=value, name=value)
	 */
	protected String getPartitionSpec(Table table, Partition partition) {
		String partitionSpec = "";

		for (int i = 0; i < table.getPartitionKeysSize(); ++i) {
			FieldSchema p = table.getPartitionKeys().get(i);

			String specAppend;

			if (quotedTypes.contains(p.getType().toLowerCase())) {
				// add quotes to appended value
				specAppend = "'" + partition.getValues().get(i) + "'";
			} else {
				// don't quote the appended value
				specAppend = partition.getValues().get(i);
			}

			partitionSpec += p.getName() + "=" + specAppend + ",";
		}
		return StringUtils.stripEnd(partitionSpec, ",");
	}

	/**
	 * Handler for a Drop Table event
	 */
	public void onDropTable(DropTableEvent tableEvent) throws MetaException {
		super.onDropTable(tableEvent);

		if (!suppressAllDropEvents) {

			Table table = tableEvent.getTable();
			String ddl = "";

			if (table.getTableType().equals(EXTERNAL_TABLE_TYPE) && table.getSd().getLocation().startsWith("s3")) {
				ddl = String.format("drop table if exists %s", getFqtn(table));

				if (!addToAthenaQueue(ddl)) {
					LOG.error("Failed to add the DropTable event to the processing queue");
				} else {
					LOG.debug(String.format("Requested Drop of table: %s", table.getTableName()));
				}
			}
		} else {
			LOG.debug(String.format("Ignoring DropTable event as %s set to True", SUPPRESS_ALL_DROP_EVENTS));
		}
	}

	/**
	 * Handler for a CreateTable Event
	 */
	public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
		super.onCreateTable(tableEvent);

		Table table = tableEvent.getTable();
		String ddl = "";

		if (table.getTableType().equals(EXTERNAL_TABLE_TYPE) && table.getSd().getLocation().startsWith("s3")) {
			try {
				ddl = HiveUtils.showCreateTable(tableEvent.getTable());
				LOG.info("The table: " + ddl);

				if (!addToAthenaQueue(ddl)) {
					LOG.error("Failed to add the CreateTable event to the processing queue");
				} else {
					LOG.debug(String.format("Requested replication of %s to AWS Glue Catalog.", table.getTableName()));
				}
			} catch (Exception e) {
				LOG.error("Unable to get current Create Table statement for replication:" + e.getMessage());
			}
		}

	}

	/**
	 * Handler for an AddPartition event
	 */
	public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
		super.onAddPartition(partitionEvent);

		if (partitionEvent.getStatus()) {
			Table table = partitionEvent.getTable();

			if (table.getTableType().equals(EXTERNAL_TABLE_TYPE) && table.getSd().getLocation().startsWith("s3")) {
				String fqtn = getFqtn(table);

				if (fqtn != null && !fqtn.equals("")) {
					partitionEvent.getPartitionIterator().forEachRemaining(p -> {
						String partitionSpec = getPartitionSpec(table, p);

						if (p.getSd().getLocation().startsWith("s3")) {
							String addPartitionDDL = String.format(
									"alter table %s add if not exists partition(%s) location '%s'", fqtn, partitionSpec,
									translateLocationToS3Path(p.getSd().getLocation()));
							if (!addToAthenaQueue(addPartitionDDL)) {
								LOG.error("Failed to add the AddPartition event to the processing queue");
							}
						} else {
							LOG.debug(String.format("Not adding partition (%s) as it is not S3 based (location %s)",
									partitionSpec, p.getSd().getLocation()));
						}
					});
				}
			} else {
				LOG.debug(String.format("Ignoring Add Partition Event for Table %s as it is not stored on S3",
						table.getTableName()));
			}
		}
	}

	/**
	 * Handler to deal with partition drop events. Receives a single partition drop
	 * event and drops all partitions included in the event.
	 */
	public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
		super.onDropPartition(partitionEvent);
		if (!suppressAllDropEvents) {
			if (partitionEvent.getStatus()) {
				Table table = partitionEvent.getTable();

				if (table.getTableType().equals(EXTERNAL_TABLE_TYPE) && table.getSd().getLocation().startsWith("s3")) {
					String fqtn = getFqtn(table);

					if (fqtn != null && !fqtn.equals("")) {
						partitionEvent.getPartitionIterator().forEachRemaining(p -> {
							String partitionSpec = getPartitionSpec(table, p);

							if (p.getSd().getLocation().startsWith("s3")) {
								String ddl = String.format("alter table %s drop if exists partition(%s);", fqtn,
										partitionSpec);

								if (!addToAthenaQueue(ddl)) {
									LOG.error(String.format(
											"Failed to add the DropPartition event to the processing queue for specification %s",
											partitionSpec));
								} else {
									LOG.debug(String.format("Requested Drop of Partition with Specification (%s)",
											partitionSpec));
								}
							} else {
								LOG.debug(
										String.format("Not dropping partition (%s) as it is not S3 based (location %s)",
												partitionSpec, p.getSd().getLocation()));
							}
						});
					}
				} else {
					LOG.debug(String.format("Ignoring Drop Partition Event for Table %s as it is not stored on S3",
							table.getTableName()));
				}
			}
		} else {
			LOG.debug(String.format("Ignoring DropPartition event as %s set to True", SUPPRESS_ALL_DROP_EVENTS));
		}
	}

	static final boolean alterTableRequiresDropTable(final Table oldTable, final Table newTable) {
		final Set<String> oldTableColumnNames = getColumnNames(oldTable);
		final Set<String> newTableColumnNames = getColumnNames(newTable);
		final boolean allOldColumnsPresentInNewTable = oldTableColumnNames.stream()
			.allMatch(newTableColumnNames::contains);

		if (!allOldColumnsPresentInNewTable) {
			// at least one old column was removed or renamed
			return true;
		}

		final Map<String, FieldSchema> newTableColumns = newTable
			.getSd()
			.getCols()
			.stream()
			.collect(toMap(x -> x.getName(), x -> x));

		final boolean allNewColumnTypesMatchOldColumnTypes = oldTable
			.getSd()
			.getCols()
			.stream()
			.allMatch((FieldSchema oldField) -> {
				final FieldSchema newField = newTableColumns.get(oldField.getName());
				return oldField.getType() == newField.getType();
			});

		return !allNewColumnTypesMatchOldColumnTypes;
	}

	static final String createAthenaAlterTableAddColumnsStatement(final Table oldTable, final Table newTable) {
		final String fqtn = getFqtn(newTable);
		final StringBuilder ddl = new StringBuilder("alter table ")
			.append(fqtn);

		final Set<FieldSchema> oldTableColumns = new HashSet<>(oldTable.getSd().getCols());
		final List<FieldSchema> newTableColumns = newTable.getSd().getCols();

		final List<FieldSchema> newColumns = newTableColumns.stream()
			.filter(newTableColumn -> !oldTableColumns.contains(newTableColumn))
			.collect(toList());

		final StringJoiner columnJoiner = new StringJoiner(", ", " add columns (", ")");
		for (FieldSchema fieldSchema : newColumns) {
			columnJoiner.add(
				String.format(
					"%s %s",
					fieldSchema.getName(),
					fieldSchema.getType()
				)
			);
		}
		return ddl.append(columnJoiner.toString()).toString();
	}

	@Override
	public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
		final Table oldTable = tableEvent.getOldTable();
		final Table newTable = tableEvent.getNewTable();

		if (!newTable.getTableType().equals(EXTERNAL_TABLE_TYPE) || !newTable.getSd().getLocation().startsWith("s3")) {
			LOG.debug(
				String.format(
					"[AlterTableEvent] Ignoring AlterTable event for Table %s as it is not stored on S3",
					newTable.getTableName()
				)
			);
			return;
		}

		if (alterTableRequiresDropTable(oldTable, newTable)) {
			final String fqtn = getFqtn(newTable);
			String createTableSql = "";
			try {
				createTableSql = HiveUtils.showCreateTable(newTable);
			}
			catch (Exception e) {
				LOG.error("[AlterTableEvent] Unable to get new Create Table statement for AlterTable event:" + e.getMessage());
				// Nothing can be done if the Create Table statement can't be generated so just short-circuit/return.
				return;
			}
			if (addToAthenaQueue(createTableSql)) {
				LOG.debug(
					String.format(
						"[AlterTableEvent] Requested (RE-)CREATE TABLE for table: %s",
						newTable.getTableName()
					)
				);
			} else {
				LOG.error(
					String.format(
						"[AlterTableEvent] Failed to add the (RE-)CREATE TABLE to the processing queue for table: %s",
						newTable.getTableName()
					)
				);
				// No point continuing with MSCK REPAIR if CREATE TABLE statement can't be queued so just short-circuit/return.
				return;
			}

			final String msckRepairTableDdl = String.format(
				"MSCK REPAIR TABLE %s",
				fqtn
			);

			if (addToAthenaQueue(msckRepairTableDdl)) {
				LOG.debug(
					String.format(
						"[AlterTableEvent] Requested MSCK REPAIR TABLE for table: %s",
						newTable.getTableName()
					)
				);
			} else {
				LOG.error(
					String.format(
						"[AlterTableEvent] Failed to add the MSCK REPAIR TABLE to the processing queue for table: %s",
						newTable.getTableName()
					)
				);
			}
		}
		else {
			final String alterTableAddColumnsDdl = createAthenaAlterTableAddColumnsStatement(oldTable, newTable);
			if (addToAthenaQueue(alterTableAddColumnsDdl)) {
				LOG.debug(
					String.format(
						"[AlterTableEvent] Requested ALTER TABLE ADD COLUMNS for table: %s",
						newTable.getTableName()
					)
				);
			} else {
				LOG.error(
					String.format(
						"[AlterTableEvent] Failed to add the ALTER TABLE ADD COLUMNS to the processing queue for table: %s",
						newTable.getTableName()
					)
				);
			}
		}
	}
}
