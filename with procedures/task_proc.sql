CREATE OR REPLACE PROCEDURE SWIGGY_DB.COMMON.RUN_FINAL_FOR_YESTERDAY()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    yesterday DATE;
    year_str STRING;
    month_str STRING;
    day_str STRING;
    stage_path STRING;
BEGIN
    -- Calculate yesterday's date
    yesterday := CURRENT_DATE - 1;

    -- Extract parts
    year_str := TO_CHAR(yesterday, 'YYYY');
    month_str := TO_CHAR(yesterday, 'FMMM'); 
    day_str := TO_CHAR(yesterday, 'FMD'); 

    -- Construct stage path
    stage_path := '@STAGE_SCH.AWS_S3_STAGE/' || year_str || '/' || month_str || '/' || day_str || '/';

    -- Call the actual procedure with constructed path
    CALL SWIGGY_DB.COMMON.FINAL_PROCEDURE(stage_path);

    RETURN 'FINAL_PROCEDURE called with path: ' || stage_path;
END;
$$;


CREATE OR REPLACE TASK RUN_FINAL_EVERYDAY_TASK
  WAREHOUSE = ADHOC_WH
  SCHEDULE = 'USING CRON 30 21 * * * UTC'  -- 21:30 UTC = 03:00 IST
  COMMENT = 'Calls FINAL_PROCEDURE at 3:00 AM IST daily'
AS
  CALL SWIGGY_DB.COMMON.RUN_FINAL_FOR_YESTERDAY();


ALTER TASK RUN_FINAL_EVERYDAY_TASK RESUME;
