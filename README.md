# Hive Glue Catalog Sync Agent

The Hive Glue Catalog Sync Agent is a software module that can be installed and configured within a Yarn cluster that manages a Hive Metastore, and provides outbound synchronisation to the [AWS Glue Data Catalog](https://aws.amazon.com/glue). This enables you to seamlessly create objects on the AWS Catalog as they are created within your existing Hadoop/Hive environment, without having to manage any code.

This project provides a jar that implements the [MetastoreEventListener](https://hive.apache.org/javadocs/r1.2.2/api/org/apache/hadoop/hive/metastore/MetaStoreEventListener.html) interface of Hive to capture all create and drop events for tables and partitions in your Hive Metastore. It then connects to Amazon Athena in your AWS Account, and runs these same commands against the Glue Catalog, to provide syncronisation of the catalog over time.

![architecture](architecture.png)

Within the [HiveGlueCatalogSyncAgent](src/main/java/com/amazonaws/services/glue/catalog/HiveGlueCatalogSyncAgent.java), the DDL from metastore events is captured and written to a [Tape2](https://github.com/square/tape) queue, which is a disk backed queue that survives application restarts. 

![internals](internals.png)

This queue is then drained by a separate thread that writes ddl events to Amazon Athena via a JDBC connection. This architecture ensures that if your Yarn cluster becomes disconnected from the Cloud for some reason, that Catalog events will not be dropped.

## Installation

You can build the software yourself by configuring Maven and issuing `mvn package`, which will result in the binary being built to `aws-glue-catalog-sync-agent/target/HiveGlueCatalogSyncAgent-1.0-SNAPSHOT.jar`, or alternatively you can download the jar from [s3://awslabs-code-us-east-1/HiveGlueCatalogSyncAgent/HiveGlueCatalogSyncAgent-1.0-SNAPSHOT.jar](https://s3.amazonaws.com/awslabs-code-us-east-1/HiveGlueCatalogSyncAgent/HiveGlueCatalogSyncAgent-1.0-SNAPSHOT.jar) (`c11eb6f2412af66afca3aea55cbfe19f`).

## Configuration

----

Apache 2.0 Software License

see [LICENSE](LICENSE) for details

