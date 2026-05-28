# ETL Explanation

## Activity-by-activity

### 1. `Lookup_Control_Table`
Reads every active row from `control.IngestionMetadata`. Returns the array the `ForEach` iterates. `firstRowOnly = false` so all entities come back.

### 2. `ForEach_Entity` (parallel, batchCount 6)
Iterates the control rows. Non-sequential with a batch count of 6 → up to six entities load concurrently, balancing throughput against source pressure.

For each entity:

#### 2a. `Lookup_Watermark`
Fetches the last successful high-water mark for this entity. `firstRowOnly = true`.

#### 2b. `Copy_Source_to_Lake`
The core extract. Source query selects only rows newer than the watermark:

```sql
SELECT * FROM [schema].[table]
WHERE ModifiedDate > '<last_watermark>'
```

- **Sink:** Parquet on ADLS Gen2, Snappy compression, partitioned by ingest date (`bronze/<path>/yyyy/MM/dd`).
- **Partitioned read:** `DynamicRange` on the watermark column parallelizes large pulls.
- **parallelCopies = 4**, `validateDataConsistency = true`.
- **Policy:** timeout 2h, retry 3× / 60s.

#### 2c. `Update_Watermark` (on success)
Stored proc `control.usp_UpdateWatermark` advances the watermark to the run timestamp — only after a clean copy, so a failure never loses data on the next run.

#### 2d. `Log_DeadLetter` (on failure)
Wired to the Copy activity's **Failed** dependency. Calls `control.usp_LogFailure`, recording entity and error message for replay/triage.

### 3. `Execute_Databricks_Transform`
Runs after the loop **Completes** (regardless of individual failures, so good entities still progress). The notebook reads Bronze Parquet, builds Silver (cleansed Delta) and Gold (star schema) used by Power BI.

## Control table

```sql
CREATE TABLE control.IngestionMetadata (
    entity_id        INT PRIMARY KEY,
    source_schema    VARCHAR(50),
    source_table     VARCHAR(128),
    sink_path        VARCHAR(256),
    load_type        VARCHAR(20),      -- FULL | INCREMENTAL
    watermark_column VARCHAR(128),
    watermark_value  VARCHAR(50),
    is_active        BIT
);
```

## Stored procedures

```sql
CREATE PROCEDURE control.usp_UpdateWatermark
    @entity_id INT, @new_watermark VARCHAR(50)
AS
    UPDATE control.IngestionMetadata
    SET watermark_value = @new_watermark
    WHERE entity_id = @entity_id;
GO

CREATE PROCEDURE control.usp_LogFailure
    @entity_id INT, @error_message VARCHAR(4000)
AS
    INSERT INTO control.PipelineFailureLog (entity_id, error_message, logged_at)
    VALUES (@entity_id, @error_message, SYSUTCDATETIME());
GO
```

## Security
- **Managed Identity** on every linked service — no connection-string secrets in pipeline JSON.
- Secrets (where unavoidable) resolved from **Azure Key Vault** via `LS_KeyVault`.
- ADLS access via the data factory's managed identity with least-privilege RBAC on the container.
