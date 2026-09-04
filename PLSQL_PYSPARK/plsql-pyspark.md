# PL/SQL Scenario → PySpark Databricks Notebook Conversion Guide

> **How to use this file:**
> Upload this `.md` file **together with** your PL/SQL scenario `.txt` file to an AI assistant and say:
> *"Convert this PL/SQL scenario file to a PySpark Databricks `.ipynb` notebook following the instructions in the guide."*
> The AI will produce a fully correct, ready-to-run `.ipynb` file.
>
> **This is the only guide file needed. Do not upload any other guide.**

---

## 1. INPUT FORMAT — Understanding the PL/SQL Scenario File

The input `.txt` file follows this repeating pattern:

```
SCEN_TASK_NO in {N}
<SQL or PL/SQL block for task N>
SCEN_TASK_NO in {N+1}
...
```

**Rules for parsing:**
- Every `SCEN_TASK_NO in {N}` line is a **task boundary marker** — it is NOT executable code.
- Everything between two task boundary markers is the **body** of that task.
- Tasks may contain: plain SQL, PL/SQL `BEGIN...END` blocks, DDL, ODI commands, or be **empty** (no body).
- Empty tasks (no body between two markers) are **no-ops** — document them with a comment cell only.
- Tasks whose entire body is inside `/** ... **/` Oracle block comments are also **no-ops** — document them, do not execute.
- Tasks whose body contains `--` single-line commented-out SQL are treated as **dead code** — do not translate those lines. See Section 20.

---

## 2. OUTPUT FORMAT — Notebook Structure Rules

### 2.1 Cell Pairing
Every task (or logical group of tasks) must have:
1. A **Markdown cell** — heading + brief description of what the task does and which SCEN number(s) it covers.
2. A **Code cell** — the PySpark / Spark SQL implementation.

**Never mix multiple unrelated SCENs into one code cell unless they are a single atomic `BEGIN...END` block in Oracle.**

### 2.2 Notebook Metadata
Always include this metadata block in the `.ipynb`:

```json
{
  "nbformat": 4,
  "nbformat_minor": 5,
  "metadata": {
    "kernelspec": {
      "display_name": "Python 3",
      "language": "python",
      "name": "python3"
    },
    "language_info": { "name": "python", "version": "3.9.0" },
    "application/vnd.databricks.v1+notebook": {
      "dashboards": [],
      "language": "python",
      "notebookMetadata": {},
      "notebookName": "<YOUR_NOTEBOOK_NAME>"
    }
  }
}
```

### 2.3 First Two Cells — Always Required

**Cell 1 — Imports (Markdown):**
```
### Step 1 — Imports and Spark config
```

**Cell 2 — Imports (Code):**
```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType, IntegerType, DoubleType,
    DecimalType, TimestampType, DateType, BooleanType
)
from pyspark.sql.window import Window

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Add any dbutils.widgets.text(...) definitions here for Oracle bind variables
# Example: dbutils.widgets.text("V_NON_COTS_PRUNE_DAYS", "90", "Prune Days")
```

---

## 3. ORACLE → SPARK SQL TRANSLATION REFERENCE TABLE

| Oracle Construct | Spark SQL / PySpark Equivalent | Notes |
|---|---|---|
| `SYSDATE` | `current_timestamp()` | Always use `current_timestamp()` |
| `SYSDATE - N` | `current_timestamp() - INTERVAL N DAYS` | For date arithmetic |
| `TO_DATE(col, 'DD-MON-YYYY')` | `TO_DATE(col, 'dd-MMM-yyyy')` | Format pattern is case-sensitive in Spark |
| `TO_DATE(SYSDATE, 'DD-MON-YYYY')` | `current_date()` | Simplify to current_date() |
| `TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))` | `CAST(date_format(date, 'yyyyMMdd') AS BIGINT)` | Format pattern lowercase in Spark — see Section 22 |
| `SUBSTR(col, start, len)` | `SUBSTR(col, start, len)` | Identical — natively supported |
| `INSTR(col, 'char')` | `INSTR(col, 'char')` | Identical — natively supported |
| `LTRIM(RTRIM(col))` | `TRIM(col)` | Spark `TRIM` does both sides |
| `CONCAT(a, b)` | `CONCAT(a, b)` | Identical |
| `NVL(col, val)` | `COALESCE(col, val)` | Use COALESCE |
| `DECODE(col, v1, r1, v2, r2, def)` | `CASE WHEN col=v1 THEN r1 WHEN col=v2 THEN r2 ELSE def END` | Always expand to CASE |
| `CAST(col AS VARCHAR2(N CHAR))` | `CAST(col AS STRING)` | Drop the length — Spark strings are unlimited |
| `NUMBER` type | `BIGINT` or `DOUBLE` | Choose based on whether decimals are needed |
| `DATE` type | `TIMESTAMP` or `DATE` | Use TIMESTAMP for datetime columns |
| Comma-join `FROM A, B WHERE A.id = B.id` | `FROM A JOIN B ON A.id = B.id` | Always convert to explicit JOIN syntax — see Section 10 |
| `(+)` outer join `WHERE A.id = B.id(+)` | `LEFT JOIN B ON A.id = B.id` | `(+)` on right side = LEFT JOIN — applies everywhere, including inside subqueries |
| `WHERE B.id(+) = A.id` | `RIGHT JOIN B ON B.id = A.id` | `(+)` on left side = RIGHT JOIN |
| `EXECUTE IMMEDIATE 'DDL'` | `spark.sql("DDL")` | Direct Spark SQL call |
| `CREATE BITMAP INDEX ... ON table(col)` | `OPTIMIZE table ZORDER BY (col, ...)` | See Section 11 |
| `DBMS_STATS.GATHER_TABLE_STATS(...)` | `OPTIMIZE table ZORDER BY (key_cols)` | See Section 12 |
| `TRUNCATE TABLE t` | `DELETE FROM t WHERE 1=1` | Delta Lake does not support TRUNCATE via SQL |
| `COMMIT` | *(omit entirely)* | Delta Lake auto-commits on each write |
| `BEGIN ... END;` | Split into individual `spark.sql()` calls in one cell | See Section 6 |
| `ALTER PROCEDURE p COMPILE` | `raise NotImplementedError(...)` in isolated cell | See Section 14 |
| `BEGIN proc(); END;` | `raise NotImplementedError(...)` in isolated cell | See Section 14 |
| `OdiStartScen ...` | Documentation comment cell + print | See Section 15 |
| `INSERT INTO t (SELECT ...)` | `spark.sql("INSERT INTO t SELECT ...")` | See Section 19 |
| `UPDATE t SET col = literal` | `spark.sql("UPDATE t SET col = literal")` | Direct — no MERGE needed, see Section 18 |
| `UPDATE t SET col = (SELECT ...)` | Rewrite as MERGE | Correlated subquery — see Section 7 |

---

## 4. TABLE / SCHEMA NAMING RULES

### 4.1 Schema Prefix Rule
- All Oracle table names must be **lowercased**.
- If a table appears **with a schema prefix** in Oracle (e.g. `PRXBI_DW.W_SALES_ORDER_LINE_F`), always apply the lowercased prefix in Spark: `prxbi_dw.w_sales_order_line_f`.
- If a table appears **without any schema prefix** in Oracle, keep it **without a prefix** in every Spark cell — it relies on the active database context.

### 4.2 Global Consistency Rule — CRITICAL
**Before writing any cell**, scan the entire source `.txt` file and build a mental table-to-schema map:

| Found in Oracle source | Apply in Spark |
|---|---|
| `PRXBI_DW.TABLE_NAME` | `prxbi_dw.table_name` — always prefixed |
| `TABLE_NAME` (no prefix) | `table_name` — never prefixed |

**This map must be applied consistently across every single cell in the notebook.**

- Never add a schema prefix to a table that has none in Oracle — even if other tables in the same cell do have prefixes.
- Never mix: if `W_SALES_ORDER_LINE_F_TEMP` has no prefix in Oracle, it must have no prefix in every cell — in MERGE targets, USING subqueries, OPTIMIZE calls, and everywhere else.

**Wrong (inconsistent):**
```sql
-- SCEN 10: source has no prefix on temp table, but notebook adds prxbi_dw. — BUG
MERGE INTO prxbi_dw.w_sales_order_line_f_temp t   -- ❌
USING ( ... FROM prxbi_dw.w_sales_order_line_f_temp s ... )  -- ❌
```

**Correct:**
```sql
MERGE INTO w_sales_order_line_f_temp t   -- ✅ no prefix because Oracle had none
USING ( ... FROM w_sales_order_line_f_temp s ... )  -- ✅
```

### 4.3 Column Names
All column names used in Spark SQL must be **lowercased**.
Aliases and subquery names may stay uppercase if they improve readability, but lowercase is preferred.

---

## 5. MERGE STATEMENT RULES

### 5.1 Standard MERGE — Direct Translation

Oracle MERGE translates directly to Spark SQL MERGE with these adjustments:
- Use explicit `JOIN` syntax in the USING clause (no comma-joins).
- `CAST(col AS VARCHAR2(N CHAR))` → `CAST(col AS STRING)`.
- Always lowercase table and column names.

```sql
-- Oracle
MERGE INTO TARGET USING (
  SELECT A.id, B.val FROM TableA A, TableB B WHERE A.fk = B.pk
) STAGE ON (TARGET.id = STAGE.id)
WHEN MATCHED THEN UPDATE SET TARGET.col = STAGE.val

-- Spark SQL
MERGE INTO schema.target target
USING (
  SELECT a.id, b.val FROM schema.table_a a JOIN schema.table_b b ON a.fk = b.pk
) stage ON target.id = stage.id
WHEN MATCHED THEN UPDATE SET target.col = stage.val
```

### 5.2 WHEN MATCHED ... WHERE Clause (Oracle extension)

Oracle allows `WHEN MATCHED THEN UPDATE SET ... WHERE condition`. Spark SQL **does not** support a WHERE clause on the WHEN MATCHED branch. Move the condition into the ON clause:

```sql
-- Oracle (invalid in Spark)
WHEN MATCHED THEN UPDATE SET T.col = S.val WHERE T.flag = '5'

-- Spark fix: move filter into ON clause
ON (S.order_num = T.sales_order_num AND T.flag = '5')
WHEN MATCHED THEN UPDATE SET T.col = S.val
```

### 5.3 MERGE Through an Oracle View

When Oracle writes `MERGE INTO VW_SOME_VIEW TARGET ...`, it is updating the underlying base table through an Oracle updatable view. In Databricks, views are not writable. Always:
- Identify the base Delta table the view reads from (strip `VW_` prefix to find the base table name).
- Write the MERGE directly against that base table.
- Add a comment explaining the Oracle-to-Databricks mapping.

```python
# SCEN N: Oracle merges into VW_W_SALES_ORDER_LINE_F
# VW_W_SALES_ORDER_LINE_F is an Oracle updatable view over W_SALES_ORDER_LINE_F
# In Databricks: write directly to the underlying Delta table
spark.sql("""
    MERGE INTO prxbi_dw.w_sales_order_line_f target
    USING ( ... ) stage ON ...
    WHEN MATCHED THEN UPDATE SET ...
""")
```

### 5.4 Final Merge to Permanent Table — DeltaTable Python API

When the final task merges many columns (10+) from a temp table into a permanent fact table, prefer the **DeltaTable Python API** over Spark SQL MERGE for clarity and reliability:

```python
DeltaTable.forName(spark, "prxbi_dw.w_sales_order_line_f").alias("t") \
    .merge(
        spark.table("w_sales_order_line_f_temp").alias("s"),
        "t.row_wid = s.row_wid"
    ) \
    .whenMatchedUpdate(set={
        "t.col_1": "s.col_1",
        "t.col_2": "s.col_2",
        # ... all columns — no trailing comma on the last entry
    }) \
    .execute()
```

> **Note:** Do NOT put a trailing comma after the last key-value pair in the `set={}` dict.

Use `spark.sql("""MERGE INTO ...""")` for simpler merges (few columns, simple conditions).

---

## 6. BEGIN...END BLOCK RULES

### 6.1 Multiple MERGEs in a BEGIN...END

When Oracle has a `BEGIN ... END;` block containing multiple MERGE statements and COMMITs, **split each MERGE into its own `spark.sql()` call in the same Python cell**. Remove all `COMMIT;` statements.

```python
# Oracle BEGIN...END with 3 MERGEs → 3 spark.sql() calls in one cell
spark.sql("""MERGE INTO ...""")   # first merge
spark.sql("""MERGE INTO ...""")   # second merge
spark.sql("""MERGE INTO ...""")   # third merge
# No COMMIT needed — Delta Lake auto-commits
```

### 6.2 Mixed BEGIN...END (INSERT + TRUNCATE + UPDATE + MERGE)

When a `BEGIN...END` block contains a **mix** of statement types, split each statement into its own `spark.sql()` call **within the same Python cell**, preserving the original Oracle order. Remove all `COMMIT;` statements.

```python
# SCEN N: Mixed BEGIN...END — split by statement type, same cell, COMMITs removed

# Statement 1 — TRUNCATE → DELETE
spark.sql("DELETE FROM wc_sales_value_email WHERE 1=1")

# Statement 2 — INSERT INTO ... SELECT
spark.sql("""
    INSERT INTO wc_sales_value_email
    SELECT col1, col2
    FROM table_a
    JOIN table_b ON ...
""")

# Statement 3 — MERGE
spark.sql("""
    MERGE INTO w_sales_order_line_f_temp target
    USING (SELECT value, row_wid FROM wc_sales_value_email ...) stage
    ON (target.row_wid = stage.row_wid)
    WHEN MATCHED THEN UPDATE SET target.x_email_opt_out = stage.value
""")

# Statement 4 — plain literal UPDATE
spark.sql("UPDATE w_sales_order_line_f_temp SET x_email_opt_out = 'N' WHERE x_email_opt_out IS NULL")
```

**Keep all statements from one Oracle `BEGIN...END` in ONE Python cell.** Do not scatter a single block across multiple notebook cells.

### 6.3 Commented-Out BEGIN...END (`/** **/`)

When the entire body of a `BEGIN...END` is wrapped in Oracle block comments `/** ... **/`, it is an **intentional no-op**. Generate a minimal cell pair — do not translate the commented SQL:

```python
# SCEN N: BEGIN...END body is entirely inside /** **/ Oracle block comments.
# This is an intentional no-op — no Databricks implementation required.
print("[SCEN N] Skipped — commented-out no-op in source.")
```

### 6.4 Single-Line `--` Commented SQL Inside BEGIN...END

When individual statements inside a `BEGIN...END` are commented out with `--`, treat those lines as **dead code** — do not translate them. Only translate the live (uncommented) statements. Add a note for traceability:

```python
# SCEN N: The -- commented-out MERGE/UPDATE above is intentionally skipped (dead code in source).
# Only live statements are translated below.
spark.sql("UPDATE w_ar_xact_f SET x_opty_wid = '0' WHERE x_opty_wid IS NULL")
```

---

## 7. ❌ CRITICAL BUG — Correlated Scalar Subquery in UPDATE SET

**This is the single most common conversion mistake. It will always cause `AnalysisException` at runtime.**

Spark SQL / Delta Lake **cannot** use a correlated scalar subquery in the `SET` clause of an `UPDATE` statement. This includes adding `LIMIT 1` — it does not fix the error.

```sql
-- Oracle (valid)
UPDATE table_a a
SET a.col = (SELECT b.val FROM table_b b WHERE b.fk = a.pk)
WHERE a.col = '0'

-- Spark SQL (WRONG — AnalysisException even with LIMIT 1)
UPDATE prxbi_dw.table_a a
SET a.col = (SELECT b.val FROM prxbi_dw.table_b b WHERE b.fk = a.pk LIMIT 1)
WHERE a.col = '0'
```

**✅ CORRECT FIX — Always rewrite as MERGE:**

```python
spark.sql("""
    MERGE INTO prxbi_dw.table_a a
    USING (
        SELECT fk, MIN(val) AS val
        FROM prxbi_dw.table_b
        GROUP BY fk
    ) b ON b.fk = a.pk
       AND a.col = '0'
    WHEN MATCHED THEN UPDATE SET a.col = b.val
""")
```

**Apply this fix to EVERY Oracle `UPDATE ... SET col = (SELECT ...)` pattern, no exceptions.**

---

## 8. Plain UPDATE — Literal Value in SET (No Subquery)

When Oracle has a simple `UPDATE` with a **literal value or expression** in the SET clause and no correlated subquery, Spark SQL supports this directly. Do **not** rewrite it as a MERGE.

```sql
-- Oracle
UPDATE W_SALES_ORDER_LINE_F_TEMP SET X_SMART_SPACE_FLG = 'N';
UPDATE W_AR_XACT_F SET X_BENEFICIARY_ORG_WID = '0' WHERE X_BENEFICIARY_ORG_WID IS NULL;
UPDATE W_SALES_ORDER_LINE_F SET W_UPDATE_DT = SYSDATE WHERE INTEGRATION_ID IN (SELECT ...);
```

```python
# Spark — translate directly, no MERGE needed
spark.sql("UPDATE w_sales_order_line_f_temp SET x_smart_space_flg = 'N'")

spark.sql("""
    UPDATE w_ar_xact_f
    SET x_beneficiary_org_wid = '0'
    WHERE x_beneficiary_org_wid IS NULL
""")

spark.sql("""
    UPDATE w_sales_order_line_f
    SET w_update_dt = current_timestamp()
    WHERE integration_id IN (
        SELECT integration_id FROM vw_w_sales_order_line_f
        WHERE (opportunity_wid = 0 OR quote_wid = 0)
          AND w_update_dt > current_timestamp() - INTERVAL 90 DAYS
    )
""")
```

**Rule summary:**
- `UPDATE SET col = literal` → direct `spark.sql("UPDATE ...")`
- `UPDATE SET col = expression` → direct `spark.sql("UPDATE ...")`
- `UPDATE SET col = (SELECT ... FROM other WHERE other.fk = this.pk)` → **rewrite as MERGE** (Section 7)

---

## 9. ❌ CRITICAL BUG — Dead Code After `raise` or `return`

**Never place executable code after a `raise` or `return` in the same Python cell.**

```python
# WRONG — OPTIMIZE is dead code, never executes
raise NotImplementedError("Procedure not yet ported.")
spark.sql("OPTIMIZE table ...")   # ← NEVER RUNS
```

**Fix — split into two separate cells:**

```python
# Cell A — raise only
raise NotImplementedError("Procedure not yet ported.")
```

```python
# Cell B — runs independently
spark.sql("OPTIMIZE table ...")
```

**Rule:** Every `raise NotImplementedError(...)` stub must be in its **own isolated cell** with no other code before or after it.

---

## 10. COMMA-JOIN TO EXPLICIT JOIN CONVERSION

### 10.1 Basic Rule

Oracle comma-join style must always be converted to explicit `JOIN ... ON` syntax in Spark SQL.

```sql
-- Oracle (comma-join)
FROM TableA A, TableB B, TableC C
WHERE A.id = B.fk AND B.key = C.ref

-- Spark SQL (explicit JOIN)
FROM schema.table_a a
JOIN schema.table_b b ON a.id = b.fk
JOIN schema.table_c c ON b.key = c.ref
```

### 10.2 JOIN Order Rule — CRITICAL

Spark SQL processes JOINs left-to-right. A table alias is only in scope **after** it appears in the FROM/JOIN chain. Oracle comma-join allows any order; Spark explicit JOINs require correct sequence.

**Ordering algorithm (apply for every comma-join conversion):**

1. Identify which table is the **largest or most pre-filtered** — place it first in FROM.
2. Each subsequent `JOIN` may only reference aliases **already declared above it**.
3. When a table bridges two previously declared aliases, place it **after** both of those aliases.
4. Inline-view subqueries in FROM should be aliased and placed where their output is first needed by another join condition.

```sql
-- Oracle (3-table comma-join, any order valid)
FROM WC_PRODUCT_D PROD, W_INVENTORY_PRODUCT_D INV_PROD, W_SALES_ORDER_LINE_F_TEMP SALES
WHERE INV_PROD.ROW_WID = SALES.INVENTORY_PRODUCT_WID
  AND INV_PROD.INVENTORY_ORG_WID = SALES.INVENTORY_ORG_WID
  AND SUBSTR(INV_PROD.INTEGRATION_ID, 1, INSTR(INV_PROD.INTEGRATION_ID,'~') - 1) = PROD.EBS_SRC_SYS_ID

-- Spark SQL (WRONG — SALES not yet defined when INV_PROD references it)
FROM wc_product_d prod
JOIN w_inventory_product_d inv_prod ON inv_prod.row_wid = sales.inventory_product_wid  -- ERROR: sales unknown

-- Spark SQL (CORRECT — define sales first, then inv_prod, then prod)
FROM w_sales_order_line_f_temp sales
JOIN w_inventory_product_d inv_prod
     ON inv_prod.row_wid = sales.inventory_product_wid
    AND inv_prod.inventory_org_wid = sales.inventory_org_wid
JOIN wc_product_d prod
     ON SUBSTR(inv_prod.integration_id, 1, INSTR(inv_prod.integration_id, '~') - 1)
        = prod.ebs_src_sys_id
```

### 10.3 Five or More Table Comma-Joins

For 5+ table comma-joins, apply the same ordering algorithm. When a table joins to multiple previously declared aliases, place it after all of them and combine all its conditions on one JOIN clause:

```python
spark.sql("""
    SELECT ...
    FROM w_sales_order_line_f_temp sales          -- driving table
    JOIN wc_contact_d con    ON sales.x_beneficiary_contact_wid = con.row_wid
    JOIN wc_person_d per     ON con.person_wid = per.row_wid
    JOIN wc_event_d eve      ON sales.x_event_id = eve.event_numeric_code
    JOIN (                                         -- inline-view subquery aliased as b
        SELECT MAX(a.end_date_wid) AS end_date_wid, a.event_wid, a.person_wid
        FROM wc_privacy_f a
        WHERE a.comm_wid = '3' AND a.delete_flag = 'N'
          AND a.event_wid > '0' AND a.person_wid > '0'
        GROUP BY a.event_wid, a.person_wid
    ) b ON b.event_wid = eve.row_wid AND b.person_wid = per.row_wid
    JOIN wc_privacy_f c ON per.integration_id = c.integration_id   -- c joins eve, per, AND b
                        AND eve.row_wid = c.event_wid
                        AND per.row_wid = c.person_wid
                        AND c.end_date_wid = b.end_date_wid
                        AND c.comm_wid = '3'
                        AND c.delete_flag = 'N'
    WHERE ...
""")
```

---

## 11. BITMAP INDEXES → OPTIMIZE ZORDER BY

Oracle bitmap indexes have no direct equivalent in Databricks Delta Lake. Replace **all** `EXECUTE IMMEDIATE 'create bitmap index ...'` blocks — regardless of how many — with a **single** `OPTIMIZE ... ZORDER BY` call covering all indexed columns.

**Always precede every OPTIMIZE call with the SET statement below. This is mandatory, not optional.**

```python
# Oracle: N separate EXECUTE IMMEDIATE 'create bitmap index ...' statements
# Spark: one OPTIMIZE call with ALL indexed columns combined

spark.sql("SET spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled = false")
spark.sql("""
    OPTIMIZE w_sales_order_line_f_temp
    ZORDER BY (
        sales_rep_wid,
        order_status_wid,
        x_event_ed_wid,
        row_wid,
        doc_curr_code,
        loc_curr_code,
        x_product_wid,
        x_opty_wid,
        x_beneficiary_addr_wid,
        x_event_id
    )
""")
```

**Rules:**
- Collect all indexed column names from every `create bitmap index` statement in the block and put them in one `ZORDER BY`.
- This OPTIMIZE cell must come **immediately after** the temp table is created and populated.
- Never split into multiple OPTIMIZE calls — one call per table.

---

## 12. DBMS_STATS → OPTIMIZE ZORDER BY

`DBMS_STATS.GATHER_TABLE_STATS(...)` collects statistics in Oracle. In Databricks, replace it with `OPTIMIZE ... ZORDER BY (key_columns)`.

**Always precede with the mandatory SET statement (same as Section 11).**

```python
spark.sql("SET spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled = false")
spark.sql("""
    OPTIMIZE prxbi_dw.table_name
    ZORDER BY (row_wid, primary_key_col)
""")
```

Choose `ZORDER BY` columns that are the most common filter/join keys for the table.

### 12.1 ❌ CRITICAL — OPTIMIZE Cannot Run on a View

`OPTIMIZE` works **only on Delta tables**, never on views.

When Oracle runs `DBMS_STATS.GATHER_TABLE_STATS(tabname => 'VW_SOME_VIEW', ...)`:
- **Do NOT** run `OPTIMIZE schema.vw_some_view` — this will fail at runtime.
- Strip the `VW_` prefix to find the base table, then schema-prefix it and OPTIMIZE that.

```python
# Oracle: DBMS_STATS on VW_W_AR_XACT_SALES_ORDER_F
# VW_W_AR_XACT_SALES_ORDER_F is an Oracle view over W_AR_XACT_SALES_ORDER_F
# OPTIMIZE must target the underlying Delta base table, not the view

spark.sql("SET spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled = false")
spark.sql("""
    OPTIMIZE prxbi_dw.w_ar_xact_sales_order_f   -- base table, NOT the view
    ZORDER BY (integration_id, sales_order_num)
""")
```

This same rule applies to MERGE (see Section 5.3) — always write to base tables, never to views.

---

## 13. DDL TRANSLATION RULES

### 13.1 DROP TABLE
```python
# Oracle: DROP TABLE table_name
# Spark: always add IF EXISTS for idempotency
spark.sql("DROP TABLE IF EXISTS schema.table_name")
```

### 13.2 CREATE TABLE AS SELECT (CTAS)
```python
# Oracle: CREATE TABLE t AS SELECT * FROM src WHERE condition
# Spark: always add USING DELTA
spark.sql(f"""
    CREATE TABLE schema.table_temp
    USING DELTA
    AS SELECT *
    FROM schema.source_table
    WHERE w_insert_dt >= current_timestamp() - INTERVAL {prune_days} DAYS
       OR w_update_dt >= current_timestamp() - INTERVAL {prune_days} DAYS
""")
```

Always add `USING DELTA`. Never omit it.

### 13.3 TRUNCATE TABLE
```python
# Oracle: EXECUTE IMMEDIATE ('TRUNCATE TABLE t')
#      or: TRUNCATE TABLE t
# Spark: Delta Lake does not support TRUNCATE — use DELETE
spark.sql("DELETE FROM schema.table_name WHERE 1=1")
```

---

## 14. STORED PROCEDURES (ALTER PROCEDURE / BEGIN proc(); END)

Oracle stored procedures have **no equivalent** in Databricks. Handle as follows:

- Place the `ALTER PROCEDURE ... COMPILE` in its **own isolated cell** with a `raise NotImplementedError`.
- Place the `BEGIN proc(); END` in its **own separate isolated cell** with a second `raise NotImplementedError`.
- These are always **two cells**, not one.
- **Never** put any other executable code in the same cell as a `raise`.

```python
# Cell A — SCEN N: ALTER PROCEDURE compile
raise NotImplementedError(
    "[SCEN N] 'ALTER PROCEDURE PROC_NAME COMPILE' has no Databricks equivalent. "
    "Implement the procedure logic directly in PySpark cells if needed. "
    "Example: dbutils.notebook.run('./PROC_NAME', timeout_seconds=3600)"
)
```

```python
# Cell B — SCEN N+1: BEGIN proc(); END
raise NotImplementedError(
    "[SCEN N+1] 'BEGIN PROC_NAME(); END' calls an Oracle stored procedure. "
    "Implement the procedure logic directly in PySpark if needed."
)
```

If the SCEN immediately after the procedure pair has an OPTIMIZE or other task, that goes in a **third separate cell** — it must never be in the same cell as a `raise`.

---

## 15. ODI ORCHESTRATION COMMANDS (OdiStartScen)

`OdiStartScen` commands are Oracle Data Integrator pipeline triggers. They have **no code equivalent** in Databricks. Always generate a **documentation-only cell**:

```python
# SCEN N: OdiStartScen -SCEN_NAME=SCENARIO_NAME -SCEN_VERSION=XXX
# This Oracle Data Integrator (ODI) command launches an ODI scenario.
# In Databricks, trigger the equivalent job/workflow externally:
#   - Databricks Workflow job triggers
#   - Delta Live Tables pipelines (if applicable)
#   - External orchestrators: Azure Data Factory, Apache Airflow, etc.
# No code action needed here — ensure your pipeline scheduler handles signalling.
print("[SCEN N] ODI orchestration step — no Databricks implementation. Trigger externally.")
```

**Do not omit these cells.** They are required for full audit traceability.

---

## 16. NOTEBOOK WIDGET PARAMETERS (Oracle Bind Variables)

Oracle scenario files reference bind variables like `#BIAPPS.V_NON_COTS_PRUNE_DAYS`. Convert these to **Databricks widget parameters**.

**Rules:**
- Always define widgets with `dbutils.widgets.text(...)` in **Cell 2** (the imports cell).
- If a sensible default value exists, provide it as the second argument. If no default is known, use `""` and add a validation check.
- Retrieve and cast the widget value **in the cell where it is first used**, not in Cell 2.

```python
# Cell 2 — define widget with default
dbutils.widgets.text("V_NON_COTS_PRUNE_DAYS", "90", "Prune Days (non-COTS)")
```

```python
# Later cell — retrieve, cast, and use
v_non_cots_prune_days = int(dbutils.widgets.get("V_NON_COTS_PRUNE_DAYS"))

spark.sql(f"""
    CREATE TABLE w_sales_order_line_f_temp USING DELTA AS
    SELECT * FROM w_sales_order_line_f
    WHERE w_insert_dt >= current_timestamp() - INTERVAL {v_non_cots_prune_days} DAYS
       OR w_update_dt >= current_timestamp() - INTERVAL {v_non_cots_prune_days} DAYS
""")
```

**If no default is known — add validation:**
```python
dbutils.widgets.text("PARAM_NAME", "")
param_raw = dbutils.widgets.get("PARAM_NAME")
if not param_raw:
    raise ValueError("Widget PARAM_NAME is required and cannot be empty.")
param_value = int(param_raw)
```

---

## 17. ETL PRECONDITION CHECKS

When a task does a `SELECT COUNT(*) FROM ... WHERE condition` as a precondition check, convert it to a Python check that **raises an exception** if the condition fails:

```python
count = spark.sql("""
    SELECT COUNT(*) AS cnt
    FROM prxbi_dw.wc_etl_parameters
    WHERE etl_job_type = 'ORDERS_FIN'
      AND (etl_status = 'COMPLETED' OR etl_status = 'ERROR')
""").collect()[0]["cnt"]

if count == 0:
    raise ValueError("ETL precondition failed: no COMPLETED or ERROR record found.")
print(f"ETL precondition passed: {count} record(s) found.")
```

---

## 18. PLAIN UPDATE — Direct Translation (No Subquery in SET)

As stated in Section 8, plain `UPDATE` statements where the SET clause contains only literals, expressions, or function calls (no correlated subquery) translate **directly** to `spark.sql("UPDATE ...")`.

Spark SQL natively supports:
- `UPDATE t SET col = 'literal'`
- `UPDATE t SET col = 0 WHERE col IS NULL`
- `UPDATE t SET col = current_timestamp()`
- `UPDATE t SET col = current_timestamp() WHERE integration_id IN (SELECT ... FROM other_table)`

All of these are valid in Spark and need no MERGE rewrite.

---

## 19. INSERT INTO ... SELECT Translation

Oracle `INSERT INTO target (SELECT ...)` translates directly to Spark SQL `INSERT INTO`:

```sql
-- Oracle
INSERT INTO WC_SALES_VALUE_EMAIL (
  SELECT DISTINCT col1, col2 FROM table_a, table_b, table_c WHERE ...
);
```

```python
# Spark
spark.sql("""
    INSERT INTO wc_sales_value_email
    SELECT DISTINCT col1, col2
    FROM table_a
    JOIN table_b ON ...
    JOIN table_c ON ...
    WHERE ...
""")
```

**Rules:**
- Remove the outer parentheses that Oracle wraps the SELECT in.
- Convert all comma-joins inside the SELECT to explicit `JOIN ... ON` syntax (same as Section 10, including join order).
- Remove all `COMMIT;` statements.
- If a `TRUNCATE TABLE` precedes the INSERT in the same block, translate TRUNCATE to `DELETE FROM t WHERE 1=1` as a separate `spark.sql()` call above the INSERT.
- `SELECT DISTINCT` inside INSERT translates directly — no changes needed.

---

## 20. COMMENTED-OUT SQL — No-Op and Dead Code Rules

### 20.1 Oracle Block Comments (`/** **/` or `/* */`) — Whole Task

Tasks whose entire body is inside Oracle block comments are **intentional no-ops**. Generate a minimal cell pair:

**Markdown cell:**
```
### Step N — <Task name> (SCEN N) — No-op
```

**Code cell:**
```python
# SCEN N: This block exists in the Oracle scenario but contains
# only commented-out SQL (/** ... **/). It is an intentional no-op.
# No Databricks implementation required.
print("[SCEN N] Skipped — commented-out no-op in source.")
```

**Do not omit these cells.** Every SCEN must appear in the notebook for traceability.

### 20.2 Single-Line `--` Comments Inside BEGIN...END — Dead Code

When individual statements inside a `BEGIN...END` block are commented out with `--`, treat those lines as **dead code**. Do not translate them. Only translate the live statements:

```python
# SCEN N: The -- commented-out statements below are dead code in the Oracle source.
# Only the live (uncommented) statements are translated.
spark.sql("UPDATE w_ar_xact_f SET x_opty_wid = '0' WHERE x_opty_wid IS NULL")
```

---

## 21. SCALAR SUBQUERIES — WHERE They Work and Where They Don't

Spark SQL supports scalar subqueries in some positions but not others:

| Location | Oracle | Spark SQL | Action |
|---|---|---|---|
| `SELECT (SELECT ...)` | ✅ | ✅ | Translate directly |
| `WHERE col = (SELECT ...)` | ✅ | ✅ | Translate directly |
| `FROM (SELECT ...)` | ✅ | ✅ | Translate directly |
| `WHERE col IN (SELECT ...)` | ✅ | ✅ | Translate directly |
| `SET col = (SELECT ...)` | ✅ | ❌ | **Rewrite as MERGE** — Section 7 |

### 21.1 Pre-Fetching Scalar Date Subqueries

For scalar subqueries that return a single date/timestamp value (e.g., `SELECT ETL_CURRENT_EXTRACT_TIME - 1 FROM params WHERE ...`), always pre-fetch the value into a Python variable before the main query:

```python
row = spark.sql("""
    SELECT etl_current_extract_time - INTERVAL 1 DAYS AS cutoff
    FROM prxbi_dw.wc_etl_parameters
    WHERE etl_job_type = 'EOD'
    LIMIT 1
""").collect()
if not row:
    raise ValueError("No EOD record found in wc_etl_parameters.")
eod_cutoff = row[0]["cutoff"]

# Use in the main query via f-string
spark.sql(f"""
    MERGE INTO prxbi_dw.wc_badge_order_f t
    USING (
        SELECT ...
        WHERE bf.w_update_dt > TIMESTAMP '{eod_cutoff}'
    ) s ON ...
    WHEN MATCHED THEN UPDATE SET ...
""")
```

---

## 22. DATE-TO-INTEGER KEY CONVERSION

Oracle frequently converts a date to an integer key using `TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))`. The Spark equivalent is:

```sql
-- Oracle
TO_NUMBER(TO_CHAR(MIN(RETURN_ORDER.CREATED_ON_DT), 'YYYYMMDD'))

-- Spark
CAST(date_format(MIN(return_order.created_on_dt), 'yyyyMMdd') AS BIGINT)
```

Note: the format string is **lowercase** in Spark (`yyyyMMdd`), not uppercase (`YYYYMMDD`).

When wrapped in `COALESCE` with a fallback of `0`:
```sql
-- Oracle
COALESCE((CASE WHEN MIN(...) IS NULL THEN NULL
          ELSE TO_NUMBER(TO_CHAR(MIN(...),'YYYYMMDD')) END), 0)

-- Spark
COALESCE(
    CASE WHEN MIN(return_order.created_on_dt) IS NULL THEN NULL
         ELSE CAST(date_format(MIN(return_order.created_on_dt), 'yyyyMMdd') AS BIGINT)
    END,
    0
)
```

---

## 23. ORACLE `(+)` OUTER JOIN — ALL LOCATIONS

Oracle `(+)` outer join syntax can appear at the **top level of a MERGE USING clause** and also **inside nested subqueries**. The same conversion rule applies everywhere:

- `WHERE A.id = B.id(+)` → `A LEFT JOIN B ON A.id = B.id`
- `WHERE A.id(+) = B.id` → `B LEFT JOIN A ON B.id = A.id` (i.e., RIGHT join from A's perspective)

**Example — `(+)` inside a nested subquery:**

```sql
-- Oracle (inner subquery with (+))
USING (
    SELECT B.ROW_WID, COALESCE(A.X_SHARER_FLG, 'N') SHARER_FLG
    FROM (
        SELECT DISTINCT C.ROW_WID, 'Y' AS X_SHARER_FLG
        FROM W_SALES_ORDER_LINE_F_TEMP A, WC_PRODUCT_D B, WC_STAND_D C
        WHERE B.ROW_WID = A.X_PRODUCT_WID AND C.ROW_WID = A.X_STAND_WID
    ) A,
    WC_STAND_D B
    WHERE B.ROW_WID = A.ROW_WID (+)   -- (+) on right = LEFT JOIN
) STAGE ...
```

```python
# Spark — apply LEFT JOIN inside the subquery
spark.sql("""
    MERGE INTO wc_stand_d target
    USING (
        SELECT b.row_wid,
               COALESCE(a.x_sharer_flg, 'N') AS sharer_flg
        FROM (
            SELECT DISTINCT c.row_wid, 'Y' AS x_sharer_flg
            FROM w_sales_order_line_f_temp a
            JOIN wc_product_d b ON b.row_wid = a.x_product_wid
            JOIN wc_stand_d c   ON c.row_wid = a.x_stand_wid
            WHERE a.x_sfdc_order_type <> 'CANCEL'
              AND b.minor_category_code IN ('4006', '4007')
        ) a
        LEFT JOIN wc_stand_d b ON b.row_wid = a.row_wid
    ) stage ON (target.row_wid = stage.row_wid)
    WHEN MATCHED THEN UPDATE SET target.x_stand_shared_flg = stage.sharer_flg
""")
```

---

## 24. STEP NUMBERING AND MARKDOWN HEADING CONVENTION

Use this heading format for all Markdown cells:

```
### Step N — <Short description> (SCEN X)
```

Or for multi-SCEN steps:
```
### Step N — <Short description> (SCEN X / Y)
```

Number steps sequentially (1, 2, 3...). SCEN numbers in the heading must exactly match the source file task numbers.

---

## 25. COMPLETE TASK COVERAGE CHECKLIST

Before finalizing the notebook, verify every item below:

**Coverage:**
- [ ] Every `SCEN_TASK_NO in {N}` from the source file has a corresponding cell pair in the notebook
- [ ] No SCEN is skipped or silently omitted

**Schema & naming:**
- [ ] A global table-to-schema map was built before writing any cell
- [ ] Every table's schema-prefix treatment is globally consistent across all cells — no table ever gets a prefix in one cell and no prefix in another
- [ ] All table names are lowercased
- [ ] All column names are lowercased

**SQL translations:**
- [ ] All `SYSDATE` replaced with `current_timestamp()` or `current_date()`
- [ ] All `NVL(...)` replaced with `COALESCE(...)`
- [ ] All `LTRIM(RTRIM(...))` replaced with `TRIM(...)`
- [ ] All `CAST(col AS VARCHAR2(...))` replaced with `CAST(col AS STRING)`
- [ ] All `TO_NUMBER(TO_CHAR(date,'YYYYMMDD'))` replaced with `CAST(date_format(date,'yyyyMMdd') AS BIGINT)`
- [ ] All comma-joins converted to explicit `JOIN ... ON` with correct alias order
- [ ] All `(+)` outer join syntax converted — including inside nested subqueries
- [ ] All `COMMIT;` statements removed
- [ ] All correlated scalar subqueries in `UPDATE SET` rewritten as `MERGE`
- [ ] All plain literal `UPDATE SET` kept as direct `spark.sql("UPDATE ...")`

**DDL & structure:**
- [ ] All `DROP TABLE` → `DROP TABLE IF EXISTS schema.table`
- [ ] All `CREATE TABLE AS SELECT` → `CREATE TABLE USING DELTA AS SELECT`
- [ ] All `TRUNCATE TABLE` → `DELETE FROM table WHERE 1=1`
- [ ] All `EXECUTE IMMEDIATE 'create bitmap index ...'` → single `OPTIMIZE ZORDER BY` (all columns combined)
- [ ] All `DBMS_STATS.GATHER_TABLE_STATS(...)` → `OPTIMIZE ZORDER BY key cols`
- [ ] All `OPTIMIZE` calls preceded by `SET spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled = false`
- [ ] All `OPTIMIZE` calls target Delta base tables only — never views (`VW_` prefix = use base table)

**BEGIN...END blocks:**
- [ ] All `BEGIN...END` blocks split into individual `spark.sql()` calls in one cell
- [ ] Mixed `BEGIN...END` (INSERT + TRUNCATE + UPDATE + MERGE) — all statements translated in one cell in original order
- [ ] `/** **/` commented-out `BEGIN...END` tasks represented as no-op cells
- [ ] `--` commented-out SQL inside `BEGIN...END` treated as dead code — not translated

**Procedures & ODI:**
- [ ] All `ALTER PROCEDURE COMPILE` → isolated `raise NotImplementedError` cell
- [ ] All `BEGIN proc(); END` → isolated `raise NotImplementedError` cell (separate cell from the ALTER)
- [ ] All `raise NotImplementedError` cells contain no other executable code
- [ ] All OPTIMIZE/other tasks after a procedure pair are in their own separate cell
- [ ] All `OdiStartScen` → documentation-only comment cell

**Inserts & special patterns:**
- [ ] All `INSERT INTO ... (SELECT ...)` → `spark.sql("INSERT INTO ... SELECT ...")`
- [ ] All scalar date subqueries pre-fetched into Python variables
- [ ] All Oracle bind variables (`#VAR`) → `dbutils.widgets` defined in Cell 2
- [ ] All MERGE-through-view → MERGE against underlying base Delta table

**Notebook structure:**
- [ ] Cell 2 contains all widget definitions
- [ ] All `raise NotImplementedError` cells are isolated
- [ ] No trailing commas on last item in DeltaTable Python API `set={}` dicts

---

## 26. QUICK REFERENCE — DO vs DO NOT

| ✅ DO | ❌ DO NOT |
|---|---|
| Build a global table-to-schema map before writing any cell | Add a schema prefix to any table that has no prefix in Oracle |
| Apply schema prefix consistently across every cell | Mix prefixed and unprefixed references to the same table |
| Run `OPTIMIZE` on Delta base tables only | Run `OPTIMIZE` on views (names with `VW_` prefix) |
| Precede every `OPTIMIZE` with the mandatory SET statement | Call `OPTIMIZE` without the `SET` statement |
| Rewrite `UPDATE SET col = (SELECT ...)` as MERGE | Use correlated subquery in UPDATE SET |
| Keep plain `UPDATE SET col = literal` as direct `spark.sql("UPDATE ...")` | Unnecessarily rewrite simple literal UPDATEs as MERGE |
| Translate `INSERT INTO t (SELECT ...)` as `spark.sql("INSERT INTO t SELECT ...")` | Omit or skip INSERT INTO statements |
| Split ALTER PROCEDURE and BEGIN proc() into two separate isolated cells | Combine the ALTER + BEGIN pair into one cell |
| Keep all statements from one `BEGIN...END` in one Python cell | Scatter one Oracle `BEGIN...END` block across multiple notebook cells |
| Treat `--` commented-out SQL as dead code — do not translate | Translate `--` commented-out SQL blocks |
| Add `IF EXISTS` to all `DROP TABLE` | Use plain `DROP TABLE` |
| Add `USING DELTA` to all `CREATE TABLE AS SELECT` | Omit `USING DELTA` |
| Delete all `COMMIT;` statements | Keep COMMIT statements |
| Convert all `(+)` to LEFT/RIGHT JOIN — everywhere, including subqueries | Leave `(+)` syntax unconverted inside nested subqueries |
| Use `TRIM()` for `LTRIM(RTRIM())` | Use LTRIM or RTRIM separately |
| Use `COALESCE()` for `NVL()` | Use NVL |
| Pre-fetch scalar date subqueries into Python variables | Embed scalar date subqueries directly in SQL f-strings |
| Put `raise NotImplementedError` alone in its own cell | Put other executable code in the same cell as a raise |
| Put OPTIMIZE tasks in their own cell when they follow a raise | Put OPTIMIZE after a raise in the same cell |
| Drive FROM the largest filtered table first in JOINs | Reference an alias before it is defined |
| Merge VW_ view writes to the underlying Delta base table | Write MERGE directly to an Oracle-style view |
| Generate no-op cells for every commented-out SCEN | Skip or silently omit any SCEN |
| Generate ODI comment cells for every OdiStartScen | Omit ODI orchestration tasks |
| Use `CAST(date_format(date,'yyyyMMdd') AS BIGINT)` for date-to-number keys | Use `TO_NUMBER(TO_CHAR(...))` in Spark SQL |
| Lowercase format strings in `date_format(...)` (`'yyyyMMdd'`) | Use uppercase Oracle format strings (`'YYYYMMDD'`) in Spark |

---

## 27. SCEN TASK TYPE HANDLING SUMMARY

Use this table as a mental checklist when processing each SCEN:

| Task Type | How to Identify | Spark Translation |
|---|---|---|
| ETL precondition COUNT | First task, `SELECT COUNT(*)` from parameters table | `count = spark.sql(...).collect()[0]["cnt"]` + `raise ValueError` if 0 |
| Empty task | No body between two SCEN markers | No-op comment cell only |
| ODI command | `OdiStartScen ...` | Documentation comment cell + print |
| Straight MERGE | `MERGE INTO ... USING ... ON ... WHEN MATCHED` | Direct `spark.sql()` MERGE |
| Comma-join MERGE | `FROM A, B, C WHERE ...` in USING clause | Convert to explicit JOINs with correct order |
| UPDATE with correlated subquery | `UPDATE t SET col = (SELECT ... FROM other WHERE other.fk = t.pk)` | Rewrite as MERGE |
| Plain literal UPDATE | `UPDATE t SET col = 'value'` or `SET col = 0 WHERE col IS NULL` | Direct `spark.sql("UPDATE ...")` |
| BEGIN...END multi-MERGE | `BEGIN MERGE...; COMMIT; MERGE...; END;` | Multiple `spark.sql()` calls in one cell, no COMMIT |
| Mixed BEGIN...END | `BEGIN TRUNCATE/INSERT/UPDATE/MERGE ... END` | All statements as individual `spark.sql()` calls in one cell |
| INSERT INTO ... SELECT | `INSERT INTO t (SELECT ...)` | `spark.sql("INSERT INTO t SELECT ...")` — comma-joins converted |
| `--` commented SQL in BEGIN...END | Lines starting with `--` inside a BEGIN block | Dead code — do not translate, add traceability comment |
| `/** **/` commented BEGIN...END | Entire body inside `/** ... **/` | No-op cell only |
| Bitmap index creation | `EXECUTE IMMEDIATE 'create bitmap index...'` | One OPTIMIZE ZORDER BY (all columns, with SET before it) |
| DBMS_STATS on a table | `DBMS_STATS.GATHER_TABLE_STATS(tabname=>'TABLE')` | OPTIMIZE ZORDER BY key cols (with SET before it) |
| DBMS_STATS on a view | `DBMS_STATS.GATHER_TABLE_STATS(tabname=>'VW_TABLE')` | Strip VW_, OPTIMIZE the base Delta table (with SET before it) |
| DROP TABLE | `DROP TABLE t` | `DROP TABLE IF EXISTS schema.t` |
| CREATE TABLE AS SELECT | `CREATE TABLE t AS SELECT ...` | `CREATE TABLE schema.t USING DELTA AS SELECT ...` |
| TRUNCATE TABLE | `EXECUTE IMMEDIATE ('TRUNCATE TABLE t')` | `DELETE FROM schema.t WHERE 1=1` |
| ALTER PROCEDURE COMPILE | `alter procedure p compile` | Isolated `raise NotImplementedError` cell |
| BEGIN proc(); END | `BEGIN proc_name(); END;` | Isolated `raise NotImplementedError` cell (separate from ALTER) |
| MERGE through view | `MERGE INTO VW_TABLE_NAME TARGET ...` | MERGE against the underlying base Delta table |
| `(+)` in nested subquery | `WHERE A.x = B.y(+)` inside an inline view | Convert to LEFT/RIGHT JOIN at that subquery level |
| Date-to-integer key | `TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))` | `CAST(date_format(date, 'yyyyMMdd') AS BIGINT)` |
| Scalar date subquery | `WHERE col > (SELECT date_col - 1 FROM params)` | Pre-fetch into Python variable, use via f-string |
| Bind variable | `#BIAPPS.VARIABLE_NAME` | `dbutils.widgets.text(...)` in Cell 2, `dbutils.widgets.get(...)` at use site |

---

*End of Conversion Guide — Complete Edition v2.0*
*Covers 100% of PL/SQL → PySpark conversion patterns for Databricks Delta Lake.*
*Replaces all previous versions. This is the only guide file needed.*


<!--
  APPEND VERBATIM TO ALL FOUR INSTRUCTION FILES:
  sql-sparksql.md, plsql-sparksql.md, sql-pyspark.md, plsql-pyspark.md
  (in the GitHub instruction repo — see ../INSTRUCTION_ARCHITECTURE.md and the
  README in this folder). These are general, client-agnostic rules. Do NOT put
  per-user schema/source mapping data here — that is injected dynamically at
  conversion time from each user's uploaded mapping.
-->

---

## Staging Table Pattern (ODI transient objects)

When converting ODI **transient staging objects** — tables whose names follow
the ODI flow/staging prefixes `C$_`, `TC$_`, `I$_`, or `E$_`, and which are
dropped and recreated within the same package/session — **prefer Spark
temporary views or DataFrames over `CREATE TABLE` / `DROP TABLE` cycles.**

- **Default (preferred):** materialize these transient steps as
  `CREATE OR REPLACE TEMPORARY VIEW <name>` (or an intermediate DataFrame),
  so the transient object lives only for the notebook run and needs no
  `DROP TABLE ... PURGE` cleanup.
- **This is a default, not an absolute.** Some clients require **persisted**
  staging (e.g. for audit, restart/recovery, or downstream reads). If the
  **schema mapping** designates the target schema for that object as persisted
  staging (for example a `source_type` such as `internal_staging` pointing at a
  real catalog.schema meant to hold staging tables), then keep it as a real
  Delta table (`CREATE OR REPLACE TABLE ... USING DELTA`) instead of a temp
  view.
- When in doubt between the two, follow the schema mapping's intent for that
  target schema; do not silently drop data that a persisted-staging design
  expects to survive the run.

```sql
-- ✅ Default: transient C$/I$/E$ step as a temp view (no DROP needed)
CREATE OR REPLACE TEMPORARY VIEW c_customer_stg AS
SELECT ... FROM ...;

-- ✅ Only when the mapping designates this schema as persisted staging:
CREATE OR REPLACE TABLE workspace.<staging_schema>.c_customer_stg
USING DELTA AS
SELECT ... FROM ...;
```

---

## Incremental Logic (watermark / control-table driven)

When the source SQL filters against a **"previous load date" style comparison**
— e.g. columns/variables matching patterns like `CURRENT_LOAD_DT`,
`PREV_LOAD_DT`, `LAST_EXTRACT`/`CURRENT_EXTRACT` timestamps, or a lookup against
an ETL control table — **do not just copy the literal `WHERE` clause into the
Spark SQL.**

Instead, generate an idempotent, restartable incremental pattern:

1. **Read side:** filter the source using a watermark read from the existing
   control table (follow the same shape as the `ETL_LOAD_INFO`-style control
   tables already present in these source files — i.e. read the last-processed
   watermark for this target, do not hard-code a literal date).
2. **Apply side:** write changes with `MERGE INTO <target> AS T USING <source>
   AS S ON <business_key>` — `WHEN MATCHED THEN UPDATE SET ...`,
   `WHEN NOT MATCHED THEN INSERT ...` (explicit `AS T` / `AS S` aliases and
   fully-qualified columns, per the Forbidden-Patterns MERGE rules).
3. **Watermark update:** include the step that **advances the watermark** in the
   control table after a successful load. Do not omit this — a read-side filter
   without a watermark update is not a correct incremental load.

```sql
-- ✅ Shape (illustrative — resolve names from the source + schema mapping):
-- 1) read the current watermark from the control table
CREATE OR REPLACE TEMPORARY VIEW _wm AS
SELECT last_extract_ts, current_extract_ts
FROM workspace.<control_schema>.etl_load_info
WHERE target_name = '<target_table>';

-- 2) MERGE only rows changed within the watermark window
MERGE INTO workspace.<schema>.<target> AS T
USING (
    SELECT s.*
    FROM workspace.<schema>.<source> s, _wm
    WHERE s.int_insert_date >  _wm.last_extract_ts
      AND s.int_insert_date <= _wm.current_extract_ts
) AS S
ON T.<business_key> = S.<business_key>
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);

-- 3) advance the watermark after a successful load
MERGE INTO workspace.<control_schema>.etl_load_info AS T
USING (SELECT '<target_table>' AS target_name,
              current_timestamp() AS new_ts) AS S
ON T.target_name = S.target_name
WHEN MATCHED THEN UPDATE SET T.last_extract_ts = S.new_ts;
```

> The exact watermark column names, control-table name, and business keys must
> come from the actual source file and the schema mapping — the above is the
> required *shape*, not literal names to copy.

---

## FLAG Comment Formats (exact, must not drift)

Two situations require a machine-detected FLAG comment. An automated detector
matches these by **exact pattern**, so reproduce the wording **verbatim** —
including the em-dash `—` (U+2014), the single quotes around the schema name,
and the uppercase keywords. Any drift silently breaks detection.

**1. Unresolved schema** — the source references a schema that is not present in
the (dynamically injected) schema mapping, or whose mapping is blank:

```
# FLAG: UNRESOLVED SCHEMA '<schema_name>' — no mapping provided. Requires manual review before this notebook can run.
```

**2. File load step with no configured landing path** — the source clearly
performs a file load (SQL*Loader control file, `OdiOutFile` / `OdiSqlUnload`,
external table over a flat file) and there is no matching file-source mapping
entry (or the mapping's load pattern is unrecognized):

```
# FLAG: FILE LOAD STEP DETECTED — no landing path configured. Original load logic omitted, requires manual conversion.
```

In both cases, emit the FLAG as its own clearly marked cell and **continue
converting the rest of the file normally** — a flag must never abort the rest
of the conversion.

> When a file-load step **does** match a configured file-source mapping entry,
> do NOT emit the FILE LOAD flag — generate the real load cell (Auto Loader or
> COPY INTO) as instructed by the dynamically injected FILE SOURCE MAPPING
> block.
