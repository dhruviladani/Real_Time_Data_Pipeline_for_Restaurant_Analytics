-- change context
use role sysadmin;
use database swiggy_db;
use schema stage_sch;
use warehouse adhoc_wh;

create or replace table stage_sch.customer (
    customer_id text,                 
    name text,                        
    email text,                        
    mobile text,                       
    login_by_using text,               
    gender text ,                      
    dob text ,                         
    anniversary text,                  
    Rating text,
    preferences text,                  
    created_date text,                   
    modified_date text,                  

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the customer stage/raw table where data will be copied from internal stage using copy command.';

-- Stream object to capture the changes. 
create or replace stream stage_sch.customer_stm 
on table stage_sch.customer
append_only = true;

create or replace table CLEAN_SCH.CUSTOMER (
    CUSTOMER_SK NUMBER AUTOINCREMENT PRIMARY KEY,                
    CUSTOMER_ID STRING NOT NULL,                                 
    NAME STRING(100) NOT NULL,                                   
    EMAIL STRING(100),                                           
    MOBILE STRING(15),                                           
    LOGIN_BY_USING STRING(50),                                   
    GENDER STRING(10),                                           
    DOB DATE,                                                    
    ANNIVERSARY DATE,                                            
    RATING FLOAT,                                                
    PREFERENCES variant,                                        
    CREATED_DATE TIMESTAMP_TZ,
    MODIFIED_DATE TIMESTAMP_TZ,
    -- Additional audit columns
    _STG_FILE_NAME STRING,                                       
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,                           
    _STG_FILE_MD5 STRING,                                       
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP      
)
comment = 'Customer entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table.';

-- Stream object to capture the changes. 
create or replace stream CLEAN_SCH.customer_stm 
on table CLEAN_SCH.customer
comment = 'This is the stream object on customer entity to track insert, update, and delete changes';

-- Custoemr Dimension table for consumption schema
create or replace table CONSUMPTION_SCH.CUSTOMER_DIM (
    CUSTOMER_HK NUMBER PRIMARY KEY,               
    CUSTOMER_ID STRING NOT NULL,                                 
    NAME STRING(100) NOT NULL,                                   
    MOBILE STRING(15) ,                                           
    EMAIL STRING(100) ,                                         
    LOGIN_BY_USING STRING(50),
    GENDER STRING(10) ,
    DOB DATE ,
    ANNIVERSARY DATE,
    Rating float,
    PREFERENCES variant,
    EFF_START_DATE TIMESTAMP_TZ,
    EFF_END_DATE TIMESTAMP_TZ,
    IS_CURRENT BOOLEAN 
)
COMMENT = 'Customer Dimension table with SCD Type 2 handling for historical tracking.';


select * from swiggy_db.stage_sch.customer;
select * from swiggy_db.stage_sch.customer_stm;

select * from swiggy_db.clean_sch.customer;
select * from swiggy_db.clean_sch.customer_stm;

select * from swiggy_db.consumption_sch.customer_dim;

create or replace procedure swiggy_db.common.CUSTOMER_MAIN_PROCEDURE(stage_name STRING)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    result_set RESULTSET;
BEGIN

     -- first copy into work 
     
        EXECUTE IMMEDIATE
        '  copy into  stage_sch.customer (customer_id, name, email, mobile, login_by_using, gender, dob, anniversary,
                   Rating, preferences, created_date, modified_date, 
                    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as customer_id,
        t.$2::text as name,
        t.$3::text as email,
        t.$4::text as mobile,
        t.$5::text as login_by_using,
        t.$6::text as gender,
        t.$7::text as dob,
        t.$8::text as anniversary,
        t.$9::text as rating,
        t.$10::text as preferences,
        t.$11::text as created_date,
        t.$12::text as modified_date,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
       FROM ' || stage_name || ' t
         )
    FILE_FORMAT = (format_name = ''swiggy_db.STAGE_SCH.CSV_FILE_FORMAT'')
    on_error = abort_statement; ';
        
    
MERGE INTO CLEAN_SCH.CUSTOMER AS target
USING (
    SELECT 
        CUSTOMER_ID::STRING AS CUSTOMER_ID,
        NAME::STRING AS NAME,
        EMAIL::STRING(100) AS EMAIL,
        MOBILE::STRING AS MOBILE,
        LOGIN_BY_USING::STRING AS LOGIN_BY_USING,
        GENDER::STRING AS GENDER,
        TRY_TO_DATE(DOB, 'YYYY-MM-DD') AS DOB,                     
        TRY_TO_DATE(ANNIVERSARY, 'YYYY-MM-DD') AS ANNIVERSARY,     
        PREFERENCES::VARIANT AS PREFERENCES,
        rating::string AS rating,
        TO_TIMESTAMP_TZ(Created_Date, 'YYYY-MM-DD HH24:MI:SS') AS created_date,
        TO_TIMESTAMP_TZ(Modified_Date, 'YYYY-MM-DD HH24:MI:SS') AS modified_date,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    FROM STAGE_SCH.CUSTOMER_STM
) AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID
WHEN MATCHED and 
target.Name != source.name OR 
target.email != source.email OR
target.Mobile != source.Mobile OR
target.rating != source.RATING OR
not Equal_null(target.Anniversary, source.Anniversary) OR
target.PREFERENCES != source.PREFERENCES 
THEN
    UPDATE SET 
        target.NAME = source.NAME,
        target.EMAIL = source.EMAIL,
        target.MOBILE = source.MOBILE,
        target.LOGIN_BY_USING = source.LOGIN_BY_USING,
        target.rating = source.RATING,
        target.PREFERENCES = source.PREFERENCES,
        target.MODIFIED_DATE = source.MODIFIED_DATE,
        target._STG_FILE_NAME = source._STG_FILE_NAME,
        target._STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
        target._STG_FILE_MD5 = source._STG_FILE_MD5,
        target._COPY_DATA_TS = source._COPY_DATA_TS
WHEN NOT MATCHED THEN
    INSERT (
        CUSTOMER_ID,
        NAME,
        EMAIL,
        MOBILE,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        RATING,
        PREFERENCES,
        CREATED_DATE,
        MODIFIED_DATE,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    )
    VALUES (
        source.CUSTOMER_ID,
        source.NAME,
        source.EMAIL,
        source.MOBILE,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.RATING,
        source.PREFERENCES,
        source.created_date,
        source.MODIFIED_DATE,
        source._STG_FILE_NAME,
        source._STG_FILE_LOAD_TS,
        source._STG_FILE_MD5,
        source._COPY_DATA_TS
    );
      
    result_set := (  
    MERGE INTO 
    CONSUMPTION_SCH.CUSTOMER_DIM AS target
USING 
    CLEAN_SCH.CUSTOMER_STM AS source
ON 
    target.IS_CURRENT = TRUE AND
    target.CUSTOMER_ID = source.CUSTOMER_ID AND
    target.NAME = source.NAME AND
    target.MOBILE = source.MOBILE AND
    target.EMAIL = source.EMAIL AND
    Equal_null(target.Anniversary, source.Anniversary) AND
    target.PREFERENCES = source.PREFERENCES AND
    equal_null(target.rating,source.rating)
WHEN MATCHED 
    AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE

WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        CUSTOMER_HK,
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        RATING,
        PREFERENCES,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CUSTOMER_ID, source.NAME, source.MOBILE, 
            source.EMAIL, source.LOGIN_BY_USING, source.GENDER, source.DOB, 
            source.ANNIVERSARY, source.PREFERENCES))),
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.RATING,
        source.PREFERENCES,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        CUSTOMER_HK,
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        RATING,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CUSTOMER_ID, source.NAME, source.MOBILE, 
            source.EMAIL, source.LOGIN_BY_USING, source.GENDER, source.DOB, 
            source.ANNIVERSARY, source.PREFERENCES))),
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        source.RATING,
        source.CREATED_DATE,
        NULL,
        TRUE
    )
    );
        
        RETURN TABLE(result_set);
    END;
$$;

-- CALL SWIGGY_DB.COMMON.CUSTOMER_MAIN_PROCEDURE('@STAGE_SCH.AWS_S3_STAGE/2025/4/9/customer.csv');    

select * from clean_sch.customer;

select * from consumption_sch.customer_dim;
