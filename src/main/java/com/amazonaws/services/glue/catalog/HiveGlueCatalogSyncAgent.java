package com.amazonaws.services.glue.catalog;

import com.amazonaws.athena.jdbc.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.ql.exec.DDLTask.appendSerdeParams;

public class HiveGlueCatalogSyncAgent extends MetaStoreEventListener {
    private static final Logger LOG = LoggerFactory.getLogger(HiveGlueCatalogSyncAgent.class);
    private static final String GLUE_CATALOG_DROP_TABLE_IF_EXISTS = "glue.catalog.dropTableIfExists";
    private static final String GLUE_CATALOG_CREATE_MISSING_DB = "glue.catalog.createMissingDB";
    private static final String GLUE_CATALOG_USER_KEY = "glue.catalog.user.key";
    private final String ATHENA_JDBC_URL = "glue.catalog.athena.jdbc.url";
    private static final String GLUE_CATALOG_USER_SECRET = "glue.catalog.user.secret";
    private static final String GLUE_CATALOG_S3_STAGING_DIR = "glue.catalog.athena.s3.staging.dir";

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
//            p.stop();
        }
    }

    /**
     * Private class which processes the ddl queue and pushes the ddl through
     * Athena. If the Athena connection is broken, try and reconnect, and if not
     * then back off for a period of time and hope that the conneciton is fixed
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
//        public void stop() {
//            LOG.info(String.format("Stopping %s", this.getClass().getCanonicalName()));
//            try {
//                athenaConnection.close();
//            } catch (SQLException e) {
//                LOG.error(e.getMessage());
//            }
//            this.run = false;
//        }

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
                                            completed = true;
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
                                            completed = true;
                                        } catch (Throwable e2) {
                                            LOG.info("ERROR: " + e.getMessage());
                                            LOG.info("DB doesn't exist for: " + query);
                                        }
                                    } else {
                                        LOG.info("Unable to complete query: " + query);
                                        LOG.info("ERROR: " + e.getMessage());
                                    }
                                }
                            }
                        }

                } else {
                    // put the thread to sleep for a configured duration
                    try {
                        LOG.debug(String.format("DDL Queue is empty. Sleeping for %s, queue state is %s", noEventSleepDuration, ddlQueue.size()));
                        Thread.sleep(noEventSleepDuration);
                    } catch (InterruptedException e) {
                        LOG.error(e.getMessage());
                    }
                }
            }
        }
    }


    public HiveGlueCatalogSyncAgent(final Configuration conf) throws Exception {
        super(conf);
        this.config = conf;
        this.athenaURL = conf.get(ATHENA_JDBC_URL);

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
        this.info.put("s3_staging_dir", config.get(GLUE_CATALOG_S3_STAGING_DIR));
        this.info.put("log_path", "/tmp/jdbc.log");
        this.info.put("log_level", "ERROR");

        if (config.get(GLUE_CATALOG_USER_KEY) != null) {
            info.put("user", config.get(GLUE_CATALOG_USER_KEY));
            info.put("password", config.get(GLUE_CATALOG_USER_SECRET));
        } else {
            this.info.put("aws_credentials_provider_class",
                    com.amazonaws.auth.InstanceProfileCredentialsProvider.class.getName());
        }

        ddlQueue = new ConcurrentLinkedQueue<>();

        configureAthenaConnection();

        dropTableIfExists = config.getBoolean(GLUE_CATALOG_DROP_TABLE_IF_EXISTS, false);
        createMissingDB = config.getBoolean(GLUE_CATALOG_CREATE_MISSING_DB, true);

        // start the queue processor thread
        AthenaQueueProcessor athenaQueueProcessor = new AthenaQueueProcessor(this.config);
        queueProcessor = new Thread(athenaQueueProcessor, "GlueSyncThread");
        queueProcessor.start();

        // add a shutdown hook to close the connections
//        Runtime.getRuntime()
//                .addShutdownHook(new Thread(new SyncAgentShutdownRoutine(athenaQueueProcessor), "Shutdown-thread"));

        LOG.info(String.format("%s online, connected to %s", this.getClass().getCanonicalName(), this.athenaURL));
    }

    private final void configureAthenaConnection() throws SQLException, SQLTimeoutException {
        LOG.info(String.format("Connecting to Amazon Athena using endpoint %s", this.athenaURL));
        athenaConnection = DriverManager.getConnection(this.athenaURL, this.info);
    }

    // Copied from Hive's code, it's a private function so had to copy it instead of
    // reusing.
    // License: Apache-2.0
    // File:
    // https://github.com/apache/hive/blob/259db56e359990a1c2830045c423453ed65b76fc/ql/src/java/org/apache/hadoop/hive/ql/exec/DDLTask.java
    private String propertiesToString(Map<String, String> props, List<String> exclude) {
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
    private String showCreateTable(org.apache.hadoop.hive.metastore.api.Table msTbl) throws HiveException {
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

        Hive db = Hive.get();
        org.apache.hadoop.hive.ql.metadata.Table tbl = db.getTable(dbName, tableName);
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
            LOG.info("show create table: ", e);
            retVal = null;
        }

        return retVal;
    }

    private boolean addToAthenaQueue(String query) {
        try {
            ddlQueue.add(query);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        super.onDropTable(tableEvent);
    }

    public void onCreateTable(CreateTableEvent tableEvent) {
        org.apache.hadoop.hive.metastore.api.Table table = tableEvent.getTable();
        String ddl = "";

        if (table.getTableType().equals(EXTERNAL_TABLE_TYPE) && table.getSd().getLocation().startsWith("s3")) {
            try {
                ddl = showCreateTable(tableEvent.getTable());
                LOG.info("The table: " + showCreateTable(tableEvent.getTable()));
                if (!addToAthenaQueue(ddl)) {
                    LOG.error("Failed to add the CreateTable event to the processing queue");
                } else {
                    LOG.debug(String.format(
                            "Ignoring Table %s as it should not be replicated to AWS Glue Catalog. Type: %s",
                            table.getTableType(), table.getSd().getLocation()));
                }
            } catch (Exception e) {
                LOG.error("Unable to get current Create Table statement for replication:" + e.getMessage());
            }
        }

    }

    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
        if (partitionEvent.getStatus()) {
            Table table = partitionEvent.getTable();

            if (table.getTableType().equals(EXTERNAL_TABLE_TYPE) && table.getSd().getLocation().startsWith("s3")) {
                String fqtn = table.getDbName() + "." + table.getTableName();

                if (fqtn != null && !fqtn.equals("")) {
                    Iterator<Partition> iterator = partitionEvent.getPartitionIterator();

                    while (iterator.hasNext()) {
                        Partition partition = iterator.next();
                        String partitionSpec = "";

                        for (int i = 0; i < table.getPartitionKeysSize(); ++i) {
                            FieldSchema p = table.getPartitionKeys().get(i);

                            String specAppend;
                            if (p.getType().equals("string")) {
                                // add quotes to appended value
                                specAppend = "'" + partition.getValues().get(i) + "'";
                            } else {
                                // don't quote the appended value
                                specAppend = partition.getValues().get(i);
                            }

                            partitionSpec += p.getName() + "=" + specAppend + ",";
                        }
                        partitionSpec = StringUtils.stripEnd(partitionSpec, ",");

                        String addPartitionDDL = "alter table " + fqtn + " add if not exists partition(" + partitionSpec
                                + ") location '" + partition.getSd().getLocation() + "'";
                        if (!addToAthenaQueue(addPartitionDDL)) {
                            LOG.error("Failed to add the AddPartition event to the processing queue");
                        }
                    }
                }
            } else {
                LOG.debug(String.format("Ignoring Add Partition Event for Table %s as it is not stored on S3",
                        table.getTableName()));
            }
        }
    }

}
