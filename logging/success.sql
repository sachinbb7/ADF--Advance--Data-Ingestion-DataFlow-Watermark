CREATE TABLE dbo.etl_success_log
(
    success_log_id     BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name      VARCHAR(100),
    pipeline_run_id    VARCHAR(100),
    activity_name      VARCHAR(100),
    source_name        VARCHAR(100),
    file_name          VARCHAR(255) NULL,
    rows_read          BIGINT NULL,
    rows_written       BIGINT NULL,
    status             VARCHAR(20),
    logged_timestamp   DATETIME2(3)
);

CREATE OR ALTER PROCEDURE dbo.usp_Insert_Success_Log
    @PipelineName       VARCHAR(100),
    @PipelineRunId      VARCHAR(100),
    @ActivityName       VARCHAR(100),
    @SourceName         VARCHAR(100),
    @FileName           VARCHAR(255) = NULL,
    @RowsRead           BIGINT = NULL,
    @RowsWritten        BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.etl_success_log
    (
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
