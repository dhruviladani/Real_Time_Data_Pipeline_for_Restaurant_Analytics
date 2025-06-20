use role sysadmin;
use database swiggy_db;
use schema stage_sch;
use warehouse adhoc_wh;


-- this table may have additional information like picked time, accept time etc.
create or replace table stage_sch.delivery (
    delivery_id text comment 'Primary Key (Source System)',                           -- foreign key reference as text (no constraint in snowflake)
    order_id text comment 'Order FK (Source System)',                           -- foreign key reference as text (no constraint in snowflake)
    deliveryagent_id text comment 'Delivery Agent FK(Source System)',                   -- foreign key reference as text (no constraint in snowflake)
    delivery_status text,                    -- delivery status as text
    estimated_time text,                     -- estimated time as text
    delivered_time text,                     -- delivered time as text
    address_id text comment 'Customer Address FK(Source System)',                         -- foreign key reference as text (no constraint in snowflake)
    delivery_date text,                      -- delivery date as text
    created_date text,                       -- created date as text
    modified_date text,                      -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the delivery stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream stage_sch.delivery_stm 
on table stage_sch.delivery
append_only = true 
comment = 'this is the append-only stream object on delivery table that only gets delta data';

create or replace table clean_sch.delivery (
    delivery_sk INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)', -- Primary key with auto-increment
    delivery_id INT NOT NULL comment 'Primary Key (Source System)',
    order_id_fk NUMBER NOT NULL comment 'Order FK (Source System)',                        -- Foreign key reference, converted to numeric type
    delivery_agent_id_fk NUMBER NOT NULL comment 'Delivery Agent FK (Source System)',               -- Foreign key reference, converted to numeric type
    delivery_status STRING,                 -- Delivery status, stored as a string
    estimated_time STRING,                  -- Estimated time, stored as a string
    delivered_time STRING,                  -- Delevered time, stored as a string
    customer_address_id_fk NUMBER NOT NULL  comment 'Customer Address FK (Source System)',                      -- Foreign key reference, converted to numeric type
    delivery_date TIMESTAMP_TZ,                -- Delivery date, converted to timestamp
    created_date TIMESTAMP_TZ,                 -- Created date, converted to timestamp
    modified_date TIMESTAMP_TZ,                -- Modified date, converted to timestamp

    -- Audit columns with appropriate data types
    _stg_file_name STRING,                  -- Source file name
    _stg_file_load_ts TIMESTAMP,            -- Source file load timestamp
    _stg_file_md5 STRING,                   -- MD5 checksum of the source file
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Metadata timestamp
)
comment = 'Delivery entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream CLEAN_SCH.delivery_stm 
on table CLEAN_SCH.delivery
comment = 'This is the stream object on delivery agent table table to track insert, update, and delete changes';

create or replace table swiggy_db.consumption_sch.delivery_dim (
    delivery_hk NUMBER PRIMARY KEY COMMENT 'Delivery Fact HK (EDW)',               -- Hash key for unique identification
    delivery_id NUMBER NOT NULL COMMENT 'Primary Key (Source System)',             -- Business key
    order_id_fk NUMBER NOT NULL COMMENT 'Order FK (Source System)',                -- Foreign key reference to order
    delivery_agent_id_fk NUMBER NOT NULL COMMENT 'Delivery Agent FK (Source System)', -- Foreign key reference to delivery agent
    delivery_status STRING,                                                        -- Delivery status
    estimated_time STRING,                                                         -- Estimated delivery time
    delivered_time STRING,                                                         -- Actual delivery time
    customer_address_id_fk NUMBER NOT NULL COMMENT 'Customer Address FK (Source System)', -- Foreign key reference to customer address
    delivery_date TIMESTAMP_TZ,                                                       -- Delivery date
    
    -- SCD2 support columns
    eff_start_date TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,                            -- Effective start date
    eff_end_date TIMESTAMP_TZ,                                                        -- Effective end date (NULL for active record)
    is_current BOOLEAN DEFAULT TRUE                                                -- Flag for current record
)
COMMENT = 'Fact table for delivery entity with SCD2 support.';


create or replace procedure SWIGGY_DB.COMMON.DELIVERY_MAIN_PROCEDURE(stage_name STRING)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    result_set RESULTSET;
BEGIN

     -- first copy into work 
     
        EXECUTE IMMEDIATE
        '  copy into stage_sch.delivery (delivery_id,order_id, deliveryagent_id, delivery_status, 
                    estimated_time, delivered_time, address_id, delivery_date, created_date, 
                    modified_date, _stg_file_name, _stg_file_load_ts, 
                    _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as delivery_id,
        t.$2::text as order_id,
        t.$3::text as deliveryagent_id,
        t.$4::text as delivery_status,
        t.$5::text as estimated_time,
        t.$6::text as delivered_time,
        t.$7::text as address_id,
        t.$8::text as delivery_date,
        t.$9::text as created_date,
        t.$10::text as modified_date,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts 
        FROM ' || stage_name || ' t
       )
       FILE_FORMAT = (format_name = ''swiggy_db.STAGE_SCH.CSV_FILE_FORMAT'')
    on_error = abort_statement; ';

MERGE INTO 
    clean_sch.delivery AS target
USING 
    stage_sch.delivery_stm AS source
ON 
    target.delivery_id = TO_NUMBER(source.delivery_id) and
    target.order_id_fk = TO_NUMBER(source.order_id) and
    target.delivery_agent_id_fk = TO_NUMBER(source.deliveryagent_id)
WHEN MATCHED AND 
        not equal_null(target.estimated_time , source.estimated_time ) or
        not equal_null(target.delivered_time , source.delivered_time ) or
        not equal_null(target.delivery_date , source.delivery_date ) or
        target.delivery_status != source.delivery_status OR
        target.customer_address_id_fk != TO_NUMBER(source.address_id)
THEN
    -- Update the existing record with the latest data
    UPDATE SET
        target.delivery_status = source.delivery_status,
        target.estimated_time = source.estimated_time,
        target.delivered_time = source.delivered_time,
        target.customer_address_id_fk = TO_NUMBER(source.address_id)
WHEN NOT MATCHED THEN
    -- Insert new record if no match is found
    INSERT (
        delivery_id,
        order_id_fk,
        delivery_agent_id_fk,
        delivery_status,
        estimated_time,
        delivered_time, 
        customer_address_id_fk,
        delivery_date,
        created_date,
        modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        TO_NUMBER(source.delivery_id),
        TO_NUMBER(source.order_id),
        TO_NUMBER(source.deliveryagent_id),
        source.delivery_status,
        source.estimated_time,
        source.delivered_time,
        TO_NUMBER(source.address_id),
        TO_TIMESTAMP_TZ(source.delivery_date),
        TO_TIMESTAMP_TZ(source.created_date),
        TO_TIMESTAMP_TZ(source.modified_date),
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

    result_set := (  
        MERGE INTO consumption_sch.delivery_dim AS target
USING CLEAN_SCH.delivery_stm AS source
ON 
    target.IS_CURRENT = TRUE AND
    target.delivery_id = source.delivery_id AND
    target.order_id_fk = source.order_id_fk AND
    target.delivery_agent_id_fk = source.delivery_agent_id_fk AND
    target.delivery_status = source.delivery_status AND
    equal_null(target.estimated_time , source.estimated_time ) AND
    equal_null(target.delivered_time , source.delivered_time ) AND
    equal_null(target.delivery_date , source.delivery_date ) AND
    target.customer_address_id_fk = source.customer_address_id_fk
WHEN MATCHED 
    AND source.METADATA$ACTION = 'DELETE' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.eff_end_date = CURRENT_TIMESTAMP,
        target.is_current = FALSE
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        delivery_hk,
        delivery_id,
        order_id_fk,
        delivery_agent_id_fk,
        delivery_status,
        estimated_time,
        delivered_time,
        customer_address_id_fk,
        delivery_date,
        eff_start_date,
        eff_end_date,
        is_current
    )
    VALUES (
        hash(SHA1_HEX(CONCAT(source.delivery_id, source.order_id_fk, source.delivery_agent_id_fk, 
            source.delivery_status, source.estimated_time, source.delivered_time, 
            source.customer_address_id_fk, source.delivery_date))),
        source.delivery_id,
        source.order_id_fk,
        source.delivery_agent_id_fk,
        source.delivery_status,
        source.estimated_time,
        source.delivered_time,
        source.customer_address_id_fk,
        source.delivery_date,
        CURRENT_TIMESTAMP,
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        delivery_hk,
        delivery_id,
        order_id_fk,
        delivery_agent_id_fk,
        delivery_status,
        estimated_time,
        delivered_time,
        customer_address_id_fk,
        delivery_date,
        eff_start_date,
        eff_end_date,
        is_current
    )
    VALUES (
        hash(SHA1_HEX(CONCAT(source.delivery_id, source.order_id_fk, source.delivery_agent_id_fk, 
            source.delivery_status, source.estimated_time, source.delivered_time, 
            source.customer_address_id_fk, source.delivery_date))),
        source.delivery_id,
        source.order_id_fk,
        source.delivery_agent_id_fk,
        source.delivery_status,
        source.estimated_time,
        source.delivered_time,
        source.customer_address_id_fk,
        source.delivery_date,
        source.created_date, -- Use created date for the effective start date
        NULL,
        TRUE
    )
    );
        
        RETURN TABLE(result_set);
    
    END;
$$;

-- CALL SWIGGY_DB.COMMON.DELIVERY_MAIN_PROCEDURE('@STAGE_SCH.AWS_S3_STAGE/2025/4/9/delivery.csv');

select * from consumption_sch.delivery_dim;
