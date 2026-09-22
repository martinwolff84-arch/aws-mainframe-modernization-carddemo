# Databricks target for the CBACT04C migration

**Databricks execution: NOT RUN.** Everything in this directory is prepared
configuration and platform glue only; no workspace, cluster, catalog, schema or
credential value in `databricks.yml` or `job_entry.py` is real.

`job_entry.py` is a thin platform shim: it reads the four input tables, calls
the exact same `validate_inputs` + `compute` from `target/interest.py` used by
the local CLI, and writes the two output tables.

## Required owner decisions before any run

See `docs/DATABRICKS-HANDOVER.md` for the full checklist. In short, an owner
must choose and supply:

1. An approved workspace, runtime and compute (`${var.cluster_id}`,
   `${var.workspace_url}` placeholders).
2. Permitted Unity Catalog locations for the four input tables and the two
   output tables (`${var.catalog}`, `${var.schema}`), plus credentials.
3. A validated bundle deployment via the real Databricks CLI; this
   `databricks.yml` skeleton has not been validated.
4. Synthetic-data functional run evidence compared field-for-field with the
   frozen reference before any wider claim.
5. Sign-off on the write/retry semantics below.

## Write and retry behaviour

1. **Transactions first**: `run_id` is validated against
   `^[A-Za-z0-9_.-]{1,64}$` (`InputValidationError`/`MALFORMED_INPUT`) before
   it is interpolated into SQL. Rows for the same `run_id` are deleted
   (`DELETE FROM <transactions_out_table> WHERE run_id = '<run_id>'`), then
   this run's transactions are appended with `run_id` and `batch_date`
   columns via `df.write.format("delta").mode("append").saveAsTable(...)`.
   Re-running the same `run_id` is therefore idempotent for the append.
2. **Accounts second**: the accounts output snapshot table is fully
   overwritten (`mode("overwrite")`), which is idempotent by itself.
3. **Failure between the two writes**: the job raises and publishes nothing
   else. The documented retry is to re-run the job with the **same**
   `run_id`: the delete removes the earlier partial transaction append, and
   the accounts overwrite replaces whatever snapshot exists. A cross-table
   atomic commit is *not* claimed — there is a window where new transactions
   coexist with an old accounts snapshot; consumers must tolerate that or
   gate reads on a completed-run marker (owner decision).
