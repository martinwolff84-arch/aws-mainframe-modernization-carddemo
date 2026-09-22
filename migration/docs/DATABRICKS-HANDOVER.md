# Databricks handover: planned, not executed

The initial acceptance boundary is a local PySpark 3.5.6 job. No workspace, cloud
account, cluster or customer data is available. Local parity does not demonstrate
Databricks deployment, Unity Catalog integration, performance or production safety.

Devin should create a small target entry point and a proposed job configuration
after implementing the migration. Keep platform-specific I/O separate from pure
Spark transformations; do not use dbutils in the local business logic.

Before the first Databricks run, an owner must:

1. Choose an approved workspace, compute/runtime and compatible Python/Spark versions.
2. Select permitted input/output locations and credentials. Do not invent host,
   catalog, schema, volume or cluster identifiers. Use synthetic data first.
3. Validate a Declarative Automation Bundle (formerly Asset Bundle) with the real
   Databricks CLI and workspace configuration, if this deployment path is selected.
4. Execute the same functional fixtures and compare the same business fields with
   the frozen reference. Record workspace job/run evidence separately from local logs.
5. Verify the proposed Delta schema, write mode, retry/idempotency behaviour and
   treatment of failed batches. File overwrite locally is not proof of atomic
   multi-table commits or production exactly-once delivery.
6. Evaluate realistic workload scale, access controls, monitoring, rollback and
   downstream consumers before any wider migration claim.

For this demo, EBCDIC/VSAM export, CICS, account-system integration, full JCL
orchestration, security certification and mainframe retirement are outside scope.

Primary references checked 22 September 2026:
- https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html
- https://docs.databricks.com/aws/en/migration/spark
- https://docs.databricks.com/aws/en/dev-tools/bundles/
