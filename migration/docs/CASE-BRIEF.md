# Case brief: preserve financial rules while modernizing one batch

**Proposed customer headline:** Move a COBOL financial batch to PySpark, with
evidence that its business rules survive the migration.

## Business story

Legacy modernization requires more than translating syntax. The team needs to
understand embedded rules, distinguish an existing defect from a migration error,
and give a reviewer enough evidence to accept the new implementation.

The public CardDemo sample gives us one bounded example: monthly interest by
account and transaction category. It is a financial-services illustration, not an
Allianz application. Its pattern is relevant to a discussion of suitable batch
workloads; the precise Allianz programme and systems remain discovery topics.

## What the engineer delegates

Understand the original, propose a migration, implement Spark transformations,
run reference comparisons, investigate differences and return a reviewable PR.
The engineer owns scope, intended behaviour and acceptance. Tests are part of the
migration deliverable, not a substitute for producing migrated code.

## Concrete review moment

In the prepared source run the last account produces a `100.00` transaction but
its stored balance remains `900.00`. The first migration preserves that observed
behaviour so reviewers can separate equivalence from a proposed correction.

Suggested line: “The reference exposed a behaviour we need to discuss with the
code owner. We can preserve it for migration parity or approve a correction, but
we should make that decision explicitly and test the chosen outcome.”

## Evidence ladder

- Prepared now: original-source reference, synthetic scenarios, acceptance tools.
- To obtain from Devin: migrated code, actual local parity results, session and PR.
- To obtain with a workspace: actual Databricks execution and platform validation.
- To measure in a customer pilot: accepted work, complete human effort and cost
  versus the current approved workflow, across a representative task set.

No benchmark, completed migration, fleet rollout or model comparison is implied
by the preparation package. The useful demonstration is the traceable work from
brief to code, tests, explicit review decisions and a PR.
