use role sysadmin;
use database swiggy_db;
use schema stage_sch;
use warehouse adhoc_wh;

create or replace table stage_sch.orders (
    order_id text comment 'Primary Key (Source System)',                  -- primary key as text
    customer_id text comment 'Customer FK(Source System)',               -- foreign key reference as text (no constraint in snowflake)
    restaurant_id text comment 'Restaurant FK(Source System)',             -- foreign key reference as text (no constraint in snowflake)
    order_date text,                -- order date as text
    total_amount text,              -- total amount as text (no decimal constraint)
    discount_amount text,
    delivery_charges text,
    final_amount text,
    status text,                   -- status as text
    payment_method text,            -- payment method as text
    is_first_order text,
    coupon_code text,
    created_date text,              -- created date as text
    modified_date text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the order stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream stage_sch.orders_stm 
on table stage_sch.orders
append_only = true
comment = 'This is the append-only stream object on orders entity that only gets delta data';

create or replace table CLEAN_SCH.ORDERS (
    ORDER_SK NUMBER AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)',                -- Auto-incremented primary key
    ORDER_ID BIGINT UNIQUE comment 'Primary Key (Source System)',                      -- Primary key inferred as BIGINT
    CUSTOMER_ID_FK BIGINT comment 'Customer FK(Source System)',                   -- Foreign key inferred as BIGINT
    RESTAURANT_ID_FK BIGINT comment 'Restaurant FK(Source System)',                 -- Foreign key inferred as BIGINT
    ORDER_DATE TIMESTAMP_TZ,                 -- Order date inferred as TIMESTAMP
    TOTAL_AMOUNT DECIMAL(10, 2),          -- Total amount inferred as DECIMAL with two decimal places
    discount_amount DECIMAL(10, 2),
    delivery_charges DECIMAL(10, 2),
    final_amount DECIMAL(10, 2),
    STATUS STRING,                        -- Status as STRING
    PAYMENT_METHOD STRING,                -- Payment method as STRING
    is_first_order boolean,
    coupon_code string,
    created_dt timestamp_tz,                                     -- record creation date
    modified_dt timestamp_tz,                                    -- last modified date, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Order entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Stream object to capture the changes. 
create or replace stream CLEAN_SCH.ORDERS_stm 
on table CLEAN_SCH.ORDERS
comment = 'This is the stream object on ORDERS table table to track insert, update, and delete changes';

-- Create order dimension table in consumption schema
create or replace table swiggy_db.consumption_sch.orders_fact (
    order_hk NUMBER PRIMARY KEY COMMENT 'Order Dim HK (EDW)',               -- Hash key for unique identification
    order_id NUMBER NOT NULL COMMENT 'Primary Key (Source System)',         -- Business key
    customer_id_fk NUMBER COMMENT 'Customer FK (Source System)',            -- Foreign key reference to customer
    restaurant_id_fk NUMBER COMMENT 'Restaurant FK (Source System)',        -- Foreign key reference to restaurant
    order_date TIMESTAMP_TZ,                                                  -- Order date
    total_amount DECIMAL(10, 2),                                           -- Total amount with two decimal places
    discount_amount DECIMAL(10, 2),                                        -- Discount amount
    delivery_charges DECIMAL(10, 2),                                       -- Delivery charges
    final_amount DECIMAL(10, 2),                                           -- Final amount
    status STRING,                                                         -- Order status
    payment_method STRING,                                                 -- Payment method
    is_first_order BOOLEAN,                                                -- Flag for first order
    coupon_code STRING,                                                    -- Coupon code if used
    eff_start_date timestamp_tz not null,                    -- Effective start date
    eff_end_date timestamp_tz,                                                -- Effective end date (NULL for active record)
    is_current BOOLEAN DEFAULT TRUE                                        -- Flag for current record
)
COMMENT = 'Dim table for orders entity with SCD2 support.';

SELECT * FROM SWIGGY_DB.STAGE_SCH.ORDERS;
SELECT * FROM SWIGGY_DB.STAGE_SCH.ORDERS_STM;

SELECT * FROM SWIGGY_DB.CLEAN_SCH.ORDERS;
SELECT * FROM SWIGGY_DB.CLEAN_SCH.ORDERS_STM;

select * from SWIGGY_DB.CONSUMPTION_SCH.ORDERS_FACT;



create or replace procedure swiggy_db.common.ORDERS_MAIN_PROCEDURE(stage_name STRING)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    
    result_set RESULTSET;

BEGIN 
    EXECUTE IMMEDIATE
    'copy into stage_sch.orders (order_id, customer_id, restaurant_id, order_date, total_amount,                
     discount_amount,delivery_charges, final_amount, status, payment_method, is_first_order, coupon_code, 
     created_date, modified_date, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as order_id,
        t.$2::text as customer_id,
        t.$3::text as restaurant_id,
        t.$4::text as order_date,
        t.$5::text as total_amount,
        t.$6::text as discount_amount,
        t.$7::text as delivery_charges,
        t.$8::text as final_amount,
        t.$9::text as status,
        t.$10::text as payment_method,
        t.$11::text as is_first_order,
        t.$12::text as coupon_code,
        t.$13::text as created_date,
        t.$14::text as modified_date,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from '|| stage_name ||' t
)
file_format = (format_name = ''stage_sch.csv_file_format'');';

--2ND MERGE INTO CLEAN SCHEMA

MERGE INTO CLEAN_SCH.ORDERS AS target
USING STAGE_SCH.ORDERS_STM AS source
    ON target.ORDER_ID = TRY_TO_NUMBER(source.order_id) -- Match based on ORDER_ID
WHEN MATCHED and (
    target.STATUS != source.status OR
    target.PAYMENT_METHOD != source.PAYMENT_METHOD
) THEN
    -- Update existing records
    UPDATE SET
        target.STATUS = source.STATUS,
        target.PAYMENT_METHOD = source.payment_method,
        target.MODIFIED_DT = TRY_TO_TIMESTAMP_TZ(source.modified_date),
        _STG_FILE_NAME = source._STG_FILE_NAME,
        _STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
        _STG_FILE_MD5 = source._STG_FILE_MD5,
        _COPY_DATA_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        ORDER_ID,
        CUSTOMER_ID_FK,
        RESTAURANT_ID_FK,
        ORDER_DATE,
        TOTAL_AMOUNT,
        discount_amount,
        delivery_charges,
        final_amount,
        STATUS,
        PAYMENT_METHOD,
        is_first_order,
        coupon_code,
        CREATED_DT,
        MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    )
    VALUES (
        TRY_TO_NUMBER(source.order_id),
        TRY_TO_NUMBER(source.customer_id),
        TRY_TO_NUMBER(source.restaurant_id),
        TRY_TO_TIMESTAMP_TZ(source.order_date),
        TRY_TO_DECIMAL(source.total_amount),
        TRY_TO_DECIMAL(source.discount_amount),
        try_to_decimal(source.delivery_charges),
        try_to_decimal(source.final_amount),
        source.STATUS,
        source.payment_method,
        source.is_first_order,
        source.coupon_code,
        TRY_TO_TIMESTAMP_TZ(source.created_date),
        TRY_TO_TIMESTAMP_TZ(source.modified_date),
        source._STG_FILE_NAME,
        source._STG_FILE_LOAD_TS,
        source._STG_FILE_MD5,
        current_timestamp
    );

    result_set := (MERGE INTO consumption_sch.orders_fact AS target
USING CLEAN_SCH.ORDERS_stm AS source
ON 
    target.IS_CURRENT = TRUE AND
    target.order_id = source.ORDER_ID AND
    target.customer_id_fk = source.CUSTOMER_ID_FK AND
    target.restaurant_id_fk = source.RESTAURANT_ID_FK AND
    target.order_date = source.ORDER_DATE AND
    target.total_amount = source.TOTAL_AMOUNT AND
    target.discount_amount = source.discount_amount AND
    target.delivery_charges = source.delivery_charges AND
    target.final_amount = source.final_amount AND
    target.status = source.STATUS AND
    target.payment_method = source.PAYMENT_METHOD AND
    target.is_first_order = source.is_first_order AND
    equal_null(target.coupon_code, source.coupon_code) 
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
        order_hk,
        order_id,
        customer_id_fk,
        restaurant_id_fk,
        order_date,
        total_amount,
        discount_amount,
        delivery_charges,
        final_amount,
        status,
        payment_method,
        is_first_order,
        coupon_code,
        eff_start_date,
        eff_end_date,
        is_current
    )
    VALUES (
        hash(SHA1_HEX(CONCAT(source.ORDER_ID, source.CUSTOMER_ID_FK, source.RESTAURANT_ID_FK, 
            source.ORDER_DATE, source.TOTAL_AMOUNT, source.discount_amount, source.delivery_charges, 
            source.final_amount, source.STATUS, source.PAYMENT_METHOD, 
            source.is_first_order, source.coupon_code))),
        source.ORDER_ID,
        source.CUSTOMER_ID_FK,
        source.RESTAURANT_ID_FK,
        source.ORDER_DATE,
        source.TOTAL_AMOUNT,
        source.discount_amount,
        source.delivery_charges,
        source.final_amount,
        source.STATUS,
        source.PAYMENT_METHOD,
        source.is_first_order,
        source.coupon_code,
        CURRENT_TIMESTAMP,
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        order_hk,
        order_id,
        customer_id_fk,
        restaurant_id_fk,
        order_date,
        total_amount,
        discount_amount,
        delivery_charges,
        final_amount,
        status,
        payment_method,
        is_first_order,
        coupon_code,
        eff_start_date,
        eff_end_date,
        is_current
    )
    VALUES (
        hash(SHA1_HEX(CONCAT(source.ORDER_ID, source.CUSTOMER_ID_FK, source.RESTAURANT_ID_FK, 
            source.ORDER_DATE, source.TOTAL_AMOUNT, source.discount_amount, source.delivery_charges, 
            source.final_amount, source.STATUS, source.PAYMENT_METHOD, 
            source.is_first_order, source.coupon_code))),
        source.ORDER_ID,
        source.CUSTOMER_ID_FK,
        source.RESTAURANT_ID_FK,
        source.ORDER_DATE,
        source.TOTAL_AMOUNT,
        source.discount_amount,
        source.delivery_charges,
        source.final_amount,
        source.STATUS,
        source.PAYMENT_METHOD,
        source.is_first_order,
        source.coupon_code,
        source.CREATED_DT, -- Use created date for the effective start date
        NULL,
        TRUE
    ));

    return TABLE(result_set);

END;
    
$$;

-- call swiggy_db.stage_sch.load_orders_data('@swiggy_db.STAGE_SCH.AWS_S3_STAGE/2025/4/21/orders.csv');
-- CALL SWIGGY_DB.COMMON.ORDERS_MAIN_PROCEDURE('@swiggy_db.STAGE_SCH.AWS_S3_STAGE/2025/4/23/orders.csv');

-- SELECT ORDER_ID,ORDER_DATE,EFF_START_DATE FROM CONSUMPTION_SCH.ORDERS_FACT WHERE ORDER_ID = 42004;

select * from consumption_sch.orders_fact;
