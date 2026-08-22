CREATE TABLE dbo.etl_failure_log
(
    failure_log_id     BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name      VARCHAR(100),
    pipeline_run_id    VARCHAR(100),
    activity_name      VARCHAR(100),
    source_name        VARCHAR(100),
    file_name          VARCHAR(255) NULL,
    error_code         VARCHAR(100) NULL,
    error_message      NVARCHAR(MAX),
    status             VARCHAR(20),
    logged_timestamp   DATETIME2(3)
);

CREATE OR ALTER PROCEDURE dbo.usp_Insert_Failure_Log
    @PipelineName       VARCHAR(100),
    @PipelineRunId      VARCHAR(100),
    @ActivityName       VARCHAR(100),
    @SourceName         VARCHAR(100),
    @FileName           VARCHAR(255) = NULL,
    @ErrorCode          VARCHAR(100) = NULL,
    @ErrorMessage       NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.etl_failure_log
    (
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
