/*
    QuickBasket control and operational logging objects
    Target: Azure SQL Database
*/

/* ============================================================
   1. High-water mark control
   ============================================================ */

CREATE TABLE dbo.etl_watermark
(
    source_name                 VARCHAR(50)  NOT NULL,
    last_processed_timestamp    DATETIME2(3) NOT NULL,

    CONSTRAINT PK_etl_watermark PRIMARY KEY (source_name)
);
GO

INSERT INTO dbo.etl_watermark
(
    source_name,
    last_processed_timestamp
)
VALUES
(
    'orders',
    '1900-01-01 00:00:00.000'
);
GO

CREATE OR ALTER PROCEDURE dbo.usp_Update_Watermark
    @SourceName          VARCHAR(50),
    @NewWatermarkValue   DATETIME2(3)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.etl_watermark
    SET last_processed_timestamp = @NewWatermarkValue
    WHERE source_name = @SourceName;
END;
GO

/* ============================================================
   2. Success logging
   ============================================================ */

CREATE TABLE dbo.etl_success_log
(
    success_log_id       BIGINT IDENTITY(1,1) NOT NULL,
    data_factory_name    VARCHAR(200)          NULL,
    trigger_id           VARCHAR(100)          NULL,
    pipeline_name        VARCHAR(100)          NOT NULL,
    pipeline_run_id      VARCHAR(100)          NOT NULL,
    activity_name        VARCHAR(100)          NOT NULL,
    source_name          VARCHAR(100)          NULL,
    file_name            VARCHAR(255)          NULL,
    rows_read            BIGINT                NULL,
    rows_written         BIGINT                NULL,
    status               VARCHAR(20)           NOT NULL,
    logged_timestamp     DATETIME2(3)          NOT NULL,

    CONSTRAINT PK_etl_success_log PRIMARY KEY (success_log_id)
);
GO

CREATE OR ALTER PROCEDURE dbo.usp_Insert_Success_Log
    @PipelineName       VARCHAR(100),
    @PipelineRunId      VARCHAR(100),
    @ActivityName       VARCHAR(100),
    @SourceName         VARCHAR(100) = NULL,
    @FileName           VARCHAR(255) = NULL,
    @RowsRead           BIGINT = NULL,
    @RowsWritten        BIGINT = NULL,
    @DataFactoryName    VARCHAR(200) = NULL,
    @TriggerId          VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.etl_success_log
    (
        data_factory_name,
        trigger_id,
        pipeline_name,
        pipeline_run_id,
        activity_name,
        source_name,
        file_name,
        rows_read,
        rows_written,
        status,
        logged_timestamp
    )
    VALUES
    (
        @DataFactoryName,
        @TriggerId,
        @PipelineName,
        @PipelineRunId,
        @ActivityName,
        @SourceName,
        @FileName,
        @RowsRead,
        @RowsWritten,
        'SUCCEEDED',
        SYSUTCDATETIME()
    );
END;
GO

/* ============================================================
   3. Failure logging
   ============================================================ */

CREATE TABLE dbo.etl_failure_log
(
    failure_log_id       BIGINT IDENTITY(1,1) NOT NULL,
    data_factory_name    VARCHAR(200)          NULL,
    trigger_id           VARCHAR(100)          NULL,
    pipeline_name        VARCHAR(100)          NOT NULL,
    pipeline_run_id      VARCHAR(100)          NOT NULL,
    activity_name        VARCHAR(100)          NOT NULL,
    source_name          VARCHAR(100)          NULL,
    file_name            VARCHAR(255)          NULL,
    error_code           VARCHAR(100)          NULL,
    error_message        NVARCHAR(MAX)         NULL,
    status               VARCHAR(20)           NOT NULL,
    logged_timestamp     DATETIME2(3)          NOT NULL,

    CONSTRAINT PK_etl_failure_log PRIMARY KEY (failure_log_id)
);
GO

CREATE OR ALTER PROCEDURE dbo.usp_Insert_Failure_Log
    @PipelineName       VARCHAR(100),
    @PipelineRunId      VARCHAR(100),
    @ActivityName       VARCHAR(100),
    @SourceName         VARCHAR(100) = NULL,
    @FileName           VARCHAR(255) = NULL,
    @ErrorCode          VARCHAR(100) = NULL,
    @ErrorMessage       NVARCHAR(MAX) = NULL,
    @DataFactoryName    VARCHAR(200) = NULL,
    @TriggerId          VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.etl_failure_log
    (
        data_factory_name,
        trigger_id,
        pipeline_name,
        pipeline_run_id,
        activity_name,
        source_name,
        file_name,
        error_code,
        error_message,
        status,
        logged_timestamp
    )
    VALUES
    (
        @DataFactoryName,
        @TriggerId,
        @PipelineName,
        @PipelineRunId,
        @ActivityName,
        @SourceName,
        @FileName,
        @ErrorCode,
        @ErrorMessage,
        'FAILED',
        SYSUTCDATETIME()
    );
END;
GO

/* ============================================================
   4. HWM lookup used by ADF
   ============================================================ */

SELECT last_processed_timestamp AS previous_hwm
FROM dbo.etl_watermark
WHERE source_name = 'orders';
GO
