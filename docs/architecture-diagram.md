# Hive Glue Catalog Sync Agent — Architecture Diagrams

## High-Level Architecture

This diagram shows the end-to-end flow from the Hadoop/Hive environment through the sync agent to AWS services.

```mermaid
flowchart LR
    subgraph on_prem["Hadoop / Hive Environment (On-Prem or EC2/EMR)"]
        direction TB
        spark["🔷 Spark SQL / HiveQL
        DDL Operations"]
        hms["🟦 Hive Metastore Server
        (HMS)"]
        agent["⚙️ HiveGlueCatalogSyncAgent
        (MetaStoreEventListener)"]
        spark -->|"CREATE TABLE
        ALTER TABLE
        DROP TABLE
        ADD PARTITION
        DROP PARTITION"| hms
        hms -->|"onCreateTable()
        onAlterTable()
        onDropTable()
        onAddPartition()
        onDropPartition()"| agent
    end

    subgraph aws["AWS Cloud"]
        direction TB
        glue["🟧 AWS Glue
                Data Catalog"]
        cwm["📊 Amazon CloudWatch
        Metrics"]
        cwl["📝 Amazon CloudWatch
        Logs"]
        athena["🔍 Amazon Athena"]
        s3["🪣 Amazon S3
        (Table Data)"]
        athena -->|"Query"| glue
        athena -->|"Read"| s3
    end

    agent -->|"Glue API
    CreateTable / UpdateTable
    DeleteTable / BatchCreatePartition
    BatchDeletePartition"| glue
    agent -->|"PutMetricData
    (QueueDepth, SyncLag,
    Success/Failure, Throttle)"| cwm
    agent -->|"PutLogEvents
    (HIVE_METADATA_SYNC)"| cwl
    glue -.->|"Catalog metadata
    points to"| s3
```

## Internal Processing Architecture

This diagram shows the internal queue-based processing pipeline inside the sync agent.

```mermaid
flowchart TB
    subgraph events["HMS Event Handlers"]
        direction LR
        ct["onCreateTable()"]
        at["onAlterTable()"]
        dt["onDropTable()"]
        ap["onAddPartition()"]
        dp["onDropPartition()"]
    end

    subgraph eligibility["Eligibility Checks"]
        direction TB
        sync_check["isSyncEligible()
        • S3 location?
        • Not disallowed?"]
        owner_check["isTableOwnershipValid()"]
    end

    subgraph queue_system["Bounded Queue (LinkedBlockingQueue)"]
        direction LR
        queue["CatalogOperation Queue
Capacity: 50,000
(configurable)"]
    end

    subgraph rejected["Rejected Operations"]
        direction TB
        reject_log["ERROR log
+ RejectedOperations TSV logger"]
        reject_metric["QueueRejection
CloudWatch Metric"]
    end

    subgraph processor["GlueCatalogQueueProcessor (Background Thread)"]
        direction TB
        drain["Drain queue
(batch window: 60s default)"]
        extract_db["Extract CREATE_DATABASE ops
(execute first)"]
        group["Group by table
(LinkedHashMap, FIFO order)"]
        merge["Merge consecutive same-type ops
• ADD_PARTITIONS → concat
• DROP_PARTITIONS → concat
• CREATE/UPDATE → last-write-wins
• DROP → deduplicate"]
        exec["Execute against Glue API
(with exponential backoff retry)"]
        drain --> extract_db --> group --> merge --> exec
    end

    subgraph observability["Observability"]
        direction LR
        cw_metrics["CloudWatch Metrics
• QueueDepth
• BatchSize
• SyncLagMs
• BatchProcessingTimeMs
• OperationSuccess/Failure
• RetryCount
• ThrottleCount"]
        cw_logs["CloudWatch Logs
(HIVE_METADATA_SYNC)"]
    end

    subgraph glue_api["AWS Glue Data Catalog API"]
        direction LR
        create_t["CreateTable"]
        update_t["UpdateTable"]
        delete_t["DeleteTable"]
        batch_cp["BatchCreatePartition"]
        batch_dp["BatchDeletePartition"]
        create_db["CreateDatabase"]
    end

    events --> eligibility
    eligibility -->|"Eligible"| queue_system
    eligibility -->|"Disallowed"| rejected
    queue_system -->|"Queue full"| rejected
    queue_system -->|"Batch window tick"| processor
    processor --> glue_api
    processor --> observability
    rejected --> reject_metric
```

## Rendering

These diagrams use [Mermaid](https://mermaid.js.org/) syntax. You can render them:

- Directly in GitHub (Mermaid is supported in `.md` files)
- In VS Code with the "Markdown Preview Mermaid Support" extension
- At [mermaid.live](https://mermaid.live) (paste the code blocks)
- In any tool that supports Mermaid (Notion, Confluence, Docusaurus, etc.)

> **Note on AWS icons:** Native Mermaid doesn't support embedded AWS Architecture Icons.
> If you need official AWS icons, consider exporting these diagrams to
> [Draw.io](https://app.diagrams.net/) or using the
> [AWS Architecture Icons](https://aws.amazon.com/architecture/icons/) with a tool
> like PlantUML + AWS-PlantUML or Diagrams (Python `diagrams` library).
