# HMS-to-GDC Sync Agent Integration Tests

End-to-end integration tests that validate the Hive Glue Catalog Sync Agent
produces GDC catalog entries that are readable by Athena, Spark, and other
AWS analytics services.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  EMR Cluster (HMS-only, no GDC as default catalog)      │
│                                                         │
│  ┌──────────┐    ┌──────────────┐    ┌───────────────┐  │
│  │  Spark   │───▶│  Hive HMS    │───▶│  Sync Agent   │  │
│  │  Steps   │    │  (Derby/RDS) │    │  (listener)   │  │
│  └──────────┘    └──────────────┘    └──────┬────────┘  │
│                                             │           │
└─────────────────────────────────────────────┼───────────┘
                                              │ GDC API
                                              ▼
                                   ┌────────────────────┐
                                   │  Glue Data Catalog  │
                                   └────────┬───────────┘
                                            │
                                            ▼
                                   ┌────────────────────┐
                                   │  Athena Validation  │
                                   └────────────────────┘
```

## Test Flow

1. **Provision**: CDK/CloudFormation creates an EMR cluster with HMS (not GDC)
   and the sync agent JAR installed as a Hive MetaStoreEventListener.
2. **Execute**: EMR Steps run Spark SQL / HiveQL to perform catalog operations
   (create tables, add partitions, alter schemas, drop tables) for multiple
   table formats (Hive managed, Hive external, Iceberg, Parquet, ORC, CSV).
3. **Sync**: The sync agent intercepts HMS events and pushes to GDC via direct API.
4. **Validate**: A validation step queries both HMS (via Spark) and GDC (via Athena)
   and compares catalog metadata + data readability.
5. **Teardown**: Cluster and test resources are cleaned up.

## Test Scenarios

See `test-scenarios.md` for the full list of test cases.

## Prerequisites

- AWS CLI configured with appropriate permissions
- An S3 bucket for test data and EMR logs
- A VPC subnet with connectivity to AWS service endpoints (see Networking below)
- The sync agent JAR built from the parent project (`mvn package`)

## IAM Permissions

The CloudFormation template creates two IAM roles. If you bring your own roles
or need to understand what's required, here's the breakdown.

### EMR EC2 Instance Profile

The EC2 instances need the following permissions. The CFN template attaches the
`AmazonElasticMapReduceforEC2Role` managed policy plus these additional policies:

Glue Data Catalog (required by the sync agent):
- `glue:CreateDatabase`, `glue:GetDatabase`
- `glue:CreateTable`, `glue:GetTable`, `glue:UpdateTable`, `glue:DeleteTable`
- `glue:BatchCreatePartition`, `glue:BatchDeletePartition`, `glue:GetPartitions`

CloudWatch Logs (required by the sync agent's CloudWatch reporter):
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`
- `logs:DescribeLogGroups`, `logs:DescribeLogStreams`

S3 (for test data and EMR logs):
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on the test bucket

### Lake Formation

If your account uses AWS Lake Formation, the EMR EC2 instance profile role
needs additional Lake Formation permissions to create databases and tables in
the Glue Data Catalog. IAM permissions alone are not sufficient when Lake
Formation is enabled — LF applies its own authorization layer on top of IAM.

The recommended approach (least privilege):
1. Grant the EC2 instance profile role the **Database Creator** permission in
   Lake Formation. This allows the role to create new databases.
2. Grant **Data Location** permissions for all S3 paths that the sync agent
   will register tables against. This allows the role to create tables pointing
   to those locations.
3. After the sync agent creates a database, grant the role **CREATE_TABLE**,
   **ALTER**, and **DROP** permissions on that database.

Alternatively (not recommended for production):
- Add the EC2 instance profile role as a **Lake Formation administrator**. This
  bypasses all LF permission checks but grants overly broad access.

To grant Database Creator permission via the AWS CLI:
```bash
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:role/ROLE_NAME \
  --permissions CREATE_DATABASE \
  --resource '{"Catalog":{}}'
```

To grant Data Location permission:
```bash
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:role/ROLE_NAME \
  --permissions DATA_LOCATION_ACCESS \
  --resource '{"DataLocation":{"ResourceArn":"arn:aws:s3:::BUCKET_NAME"}}'
```

## Networking

The EMR cluster needs connectivity to the following AWS service endpoints:
- `logs.<region>.amazonaws.com` (CloudWatch Logs — used by the sync agent at HMS startup)
- `glue.<region>.amazonaws.com` (Glue Data Catalog APIs)
- `s3.<region>.amazonaws.com` (test data, EMR logs, bootstrap scripts)
- `elasticmapreduce.<region>.amazonaws.com` (EMR service communication)

If your subnet has internet access (IGW + public IP), this works out of the box.

If you're using VPC endpoints, ensure the endpoint security groups allow inbound
TCP 443 from the EMR cluster's security group or subnet CIDR. The sync agent
initializes the CloudWatch Logs client during HMS startup — if this connection
times out, the Hive Metastore will fail to start entirely.

## Usage

```bash
# Build the sync agent JAR first
cd .. && mvn package -DskipTests && cd integration-tests

# Deploy test infrastructure
cdk deploy

# Run integration tests (submits EMR steps and validates)
./run-tests.sh

# Teardown
cdk destroy
```
