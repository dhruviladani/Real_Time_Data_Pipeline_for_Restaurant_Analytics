use role sysadmin;
use warehouse adhoc_wh;
use database swiggy_db;
use schema stage_sch;

create or replace table swiggy_db.stage_sch.login_audit (
    loginid text,
    customerid text,
    logintype text,
    deviceinterface text,
    mobiledevicename text,
    webinterface text,
    lastlogin text,
    -- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the login_audit stage/raw table where data will be copied from internal stage using copy command. This is as-is data representation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

-- Create append-only stream on stage table
create or replace stream swiggy_db.stage_sch.login_audit_stm 
on table stage_sch.login_audit
append_only = true
comment = 'This is the append-only stream object on login_audit table that gets delta data based on changes';

-- Create clean layer table with proper data types
create or replace table swiggy_db.clean_sch.login_audit (
    login_sk number autoincrement primary key,
    login_id number not null unique,
    customer_id number not null,
    login_type string(20) not null,
    device_interface string(50),
    mobile_device_name string(100),
    web_interface string(100),
    last_login timestamp_tz not null,
    device_category string(20),
    is_mobile boolean not null default true,
    device_os string(20),
    login_hour number(2),
    login_day string(10),
    login_month string(10),
    login_year number(4),
    
    -- additional audit columns
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
)
comment = 'Login audit entity under clean schema with appropriate data types. This table tracks user login activities with enriched information about devices and time dimensions.';

-- Create standard stream on clean table
create or replace stream swiggy_db.clean_sch.login_audit_stm 
on table clean_sch.login_audit
comment = 'This is a standard stream object on the login_audit table to track insert, update, and delete changes';

-- Create fact table for login audit in consumption layer
create or replace table swiggy_db.consumption_sch.login_audit_fact (
    login_hk NUMBER primary key,                       -- hash key for the fact
    login_id number(38,0) not null,                   -- business key
    customer_id number(38,0) not null,                -- foreign key to customer dimension
    login_type varchar(20) not null,                  -- app, web, etc.
    device_interface varchar(50),                     -- iOS, Android, etc.
    mobile_device_name varchar(100),                  -- device name
    web_interface varchar(100),                       -- browser details
    last_login timestamp_tz(9) not null,              -- login timestamp
    
    -- enriched attributes
    device_category varchar(20),                      -- device category
    is_mobile boolean not null default true,          -- mobile flag
    device_os varchar(20),                            -- device OS
    
    -- time dimensions
    login_hour number(2),                             -- hour of day (0-23)
    login_day varchar(10),                            -- day of week
    login_month varchar(10),                          -- month name
    login_year number(4),                             -- year
    
    -- time dimension keys (for joining to time dimension if exists)
    date_key number,                                  -- foreign key to date dimension
    time_key number,                                  -- foreign key to time dimension
    
    -- ETL metadata
    eff_start_date timestamp_tz(9) not null default current_timestamp(),
    eff_end_date timestamp_tz(9)
)
comment = 'Fact table for login audit events with time dimension attributes';

CREATE OR REPLACE PROCEDURE swiggy_db.common.LOGIN_AUDIT_MAIN_PROCEDURE(stage_name STRING)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    result_set RESULTSET;
BEGIN

            EXECUTE IMMEDIATE
            '-- Copy data from stage to table
copy into swiggy_db.stage_sch.login_audit 
from (
    select 
        $1 as LoginID,
        $2::text as CustomerID, 
        $3::text as LoginType, 
        $4::text as DeviceInterface,
        $5::text as MobileDeviceName,
        $6::text as WebInterface,
        $7::text as LastLogin,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from '|| stage_name ||'
)
FILE_FORMAT = (format_name = ''swiggy_db.STAGE_SCH.CSV_FILE_FORMAT'');';

--2ND MERGE INTO CLEAN SCHEMA

MERGE INTO swiggy_db.clean_sch.login_audit AS target
USING (
    SELECT 
        CAST(LoginID AS NUMBER) AS login_id,
        CAST(CustomerID AS NUMBER) AS customer_id,
        CAST(LoginType AS STRING) AS login_type,
        CAST(DeviceInterface AS STRING) AS device_interface,
        CAST(MobileDeviceName AS STRING) AS mobile_device_name,
        CAST(WebInterface AS STRING) AS web_interface,
        TO_TIMESTAMP_TZ(LastLogin, 'YYYY-MM-DD HH24:MI:SS') AS last_login,
        
        -- Device category derivation
        CASE
            WHEN DeviceInterface = 'iOS' THEN 'Apple'
            WHEN DeviceInterface = 'Android' THEN 'Android'
            WHEN WebInterface IS NOT NULL AND WebInterface != '' THEN 'Web'
            ELSE 'Unknown'
        END AS device_category,
        
        -- Is mobile flag
        CASE
            WHEN DeviceInterface IN ('iOS', 'Android') OR MobileDeviceName IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_mobile,
        
        -- Device OS
        CASE
            WHEN DeviceInterface = 'iOS' THEN 'iOS'
            WHEN DeviceInterface = 'Android' THEN 'Android'
            WHEN WebInterface IS NOT NULL AND WebInterface != '' THEN 'Web'
            ELSE 'Unknown'
        END AS device_os,
        
        -- Time dimensions for analytics
        EXTRACT(HOUR FROM TO_TIMESTAMP_TZ(LastLogin, 'YYYY-MM-DD HH24:MI:SS')) AS login_hour,
        DAYNAME(TO_TIMESTAMP_TZ(LastLogin, 'YYYY-MM-DD HH24:MI:SS')) AS login_day,
        MONTHNAME(TO_TIMESTAMP_TZ(LastLogin, 'YYYY-MM-DD HH24:MI:SS')) AS login_month,
        EXTRACT(YEAR FROM TO_TIMESTAMP_TZ(LastLogin, 'YYYY-MM-DD HH24:MI:SS')) AS login_year,
        
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM stage_sch.login_audit_stm
) AS source
ON target.login_id = source.login_id
WHEN MATCHED AND (
    target.customer_id != source.customer_id OR
    target.login_type != source.login_type OR
    not equal_null(target.device_interface, source.device_interface) OR
    not equal_null(target.mobile_device_name, source.mobile_device_name) OR
    not equal_null(target.web_interface , source.web_interface) OR
    target.last_login != source.last_login
) THEN 
    UPDATE SET 
        target.customer_id = source.customer_id,
        target.login_type = source.login_type,
        target.device_interface = source.device_interface,
        target.mobile_device_name = source.mobile_device_name,
        target.web_interface = source.web_interface,
        target.last_login = source.last_login,
        target.device_category = source.device_category,
        target.is_mobile = source.is_mobile,
        target.device_os = source.device_os,
        target.login_hour = source.login_hour,
        target.login_day = source.login_day,
        target.login_month = source.login_month,
        target.login_year = source.login_year,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    INSERT (
        login_id,
        customer_id,
        login_type,
        device_interface,
        mobile_device_name,
        web_interface,
        last_login,
        device_category,
        is_mobile,
        device_os,
        login_hour,
        login_day,
        login_month,
        login_year,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        source.login_id,
        source.customer_id,
        source.login_type,
        source.device_interface,
        source.mobile_device_name,
        source.web_interface,
        source.last_login,
        source.device_category,
        source.is_mobile,
        source.device_os,
        source.login_hour,
        source.login_day,
        source.login_month,
        source.login_year,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

result_set := (MERGE INTO swiggy_db.consumption_sch.login_audit_fact AS target
USING swiggy_db.clean_sch.login_audit_stm AS source
ON target.login_id = source.login_id 
WHEN MATCHED AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET 
        target.customer_id = source.customer_id,
        target.login_type = source.login_type,
        target.device_interface = source.device_interface,
        target.mobile_device_name = source.mobile_device_name,
        target.web_interface = source.web_interface,
        target.last_login = source.last_login,
        target.device_category = source.device_category,
        target.is_mobile = source.is_mobile,
        target.device_os = source.device_os,
        target.login_hour = source.login_hour,
        target.login_day = source.login_day,
        target.login_month = source.login_month,
        target.login_year = source.login_year,
        target.date_key = TO_NUMBER(TO_CHAR(source.last_login, 'YYYYMMDD')),
        target.time_key = TO_NUMBER(TO_CHAR(source.last_login, 'HH24MISS')),
        target.eff_end_date = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (
        login_hk,
        login_id,
        customer_id,
        login_type,
        device_interface,
        mobile_device_name,
        web_interface,
        last_login,
        device_category,
        is_mobile,
        device_os,
        login_hour,
        login_day,
        login_month,
        login_year,
        date_key,
        time_key,
        eff_start_date,
        eff_end_date
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.login_id::text, source.customer_id::text, source.last_login::text))),
        source.login_id,
        source.customer_id,
        source.login_type,
        source.device_interface,
        source.mobile_device_name,
        source.web_interface,
        source.last_login,
        source.device_category,
        source.is_mobile,
        source.device_os,
        source.login_hour,
        source.login_day,
        source.login_month,
        source.login_year,
        TO_NUMBER(TO_CHAR(source.last_login, 'YYYYMMDD')),
        TO_NUMBER(TO_CHAR(source.last_login, 'HH24MISS')),
        source.last_login,
        NULL
    ));
return TABLE(result_set);
END;
$$;

-- CALL swiggy_db.common.LOGIN_AUDIT_MAIN_PROCEDURE('@SWIGGY_DB.STAGE_SCH.AWS_S3_STAGE/2025/4/9/login_audit_new.csv');

select * from swiggy_db.consumption_sch.login_audit_fact;
