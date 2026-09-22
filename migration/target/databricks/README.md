# Databricks target for the CBACT04C migration

**Databricks execution: NOT RUN.** Everything in this directory is prepared
configuration and platform glue only; no workspace, cluster, catalog, schema or
credential value in `databricks.yml` or `job_entry.py` is real.

`job_entry.py` is a thin platform shim: it reads the four input tables, calls
the exact same `validate_inputs` + `compute` from `target/interest.py` used by
the local CLI, and writes the two output tables.

Parameter resolution: if the job is launched with arguments (`sys.argv`),
argparse requires every parameter; otherwise dbutils widgets are **read only**
— missing widgets raise on `.get()` and count as absent, and absent required
parameters are rejected as `MALFORMED_INPUT` (widgets are never created
implicitly). `run_id` must match `^[A-Za-z0-9_.-]{1,64}$` and every table name
must match `^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){0,2}$` before being interpolated
(backticked per part) into SQL.

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
   `^[A-Za-z0-9_.-]{1,64}$` and every table name against
   `^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){0,2}$` (`InputValidationError`/
   `MALFORMED_INPUT`) before interpolation. The run's transactions are
   written in **one atomic Delta commit**:
   `df.write.format("delta").mode("overwrite")
   .option("replaceWhere", "run_id = '<run_id>'").saveAsTable(...)` —
   replaceWhere substitutes exactly this run's rows in a single transaction,
   so the previous run's rows remain intact until the new commit succeeds
   and a failed retry does not erase them. `saveAsTable` also creates the
   table when absent; replaceWhere-on-create behaviour must be confirmed in
   the owner's workspace since nothing here was executed.
2. **Accounts second**: the accounts output snapshot table is fully
   overwritten (`mode("overwrite")`), which is idempotent by itself.
3. **Failure between the two writes**: the job raises and publishes nothing
   else. The documented retry is to re-run the job with the **same**
   `run_id` — the atomic replaceWhere makes the transactions write
   idempotent and the accounts overwrite replaces whatever snapshot exists.
   A cross-table atomic commit is *not* claimed — there is a window where
   new transactions coexist with an old accounts snapshot; consumers must
   tolerate that or gate reads on a completed-run marker (owner decision).
