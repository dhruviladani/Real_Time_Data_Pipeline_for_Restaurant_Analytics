use role sysadmin;
use database swiggy_db;
use schema stage_sch;
use warehouse adhoc_wh;

create or replace table stage_sch.orderitem (
    orderitemid text comment 'Primary Key (Source System)',              -- primary key as text
    orderid text comment 'Order FK(Source System)',                  -- foreign key reference as text (no constraint in snowflake)
    menuitemid text comment 'Menu FK(Source System)',                   -- foreign key reference as text (no constraint in snowflake)
    quantity text,                 -- quantity as text
    price text,                    -- price as text (no decimal constraint)
    subtotal text,                 -- subtotal as text (no decimal constraint)
    ratings text,
    createddate text,              -- created date as text
    modifieddate text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the order item stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream stage_sch.orderitem_stm 
on table stage_sch.orderitem
append_only = true
comment = 'This is the append-only stream object on order item table that only gets delta data';

create or replace table clean_sch.order_item (
    order_item_sk NUMBER AUTOINCREMENT primary key comment 'Surrogate Key (EDW)',    -- Auto-incremented unique identifier for each order item
    order_item_id NUMBER  NOT NULL UNIQUE comment 'Primary Key (Source System)',
    order_id_fk NUMBER  NOT NULL comment 'Order FK(Source System)',                  -- Foreign key reference for Order ID
    menuitem_id_fk NUMBER  NOT NULL comment 'Menu FK(Source System)',                   -- Foreign key reference for Menu ID
    quantity NUMBER(10, 2),                 -- Quantity as a decimal number
    price NUMBER(10, 2),                    -- Price as a decimal number
    subtotal NUMBER(10, 2),                 -- Subtotal as a decimal number
    ratings NUMBER(10, 2),
    created_dt TIMESTAMP_TZ,                 -- Created date of the order item
    modified_dt TIMESTAMP_TZ,                -- Modified date of the order item

    -- Audit columns
    _stg_file_name VARCHAR(255),            -- File name of the staging file
    _stg_file_load_ts TIMESTAMP,            -- Timestamp when the file was loaded
    _stg_file_md5 VARCHAR(255),             -- MD5 hash of the file for integrity check
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when data is copied into the clean layer
)
comment = 'Order item entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream CLEAN_SCH.order_item_stm 
on table CLEAN_SCH.order_item
comment = 'This is the stream object on order_item table table to track insert, update, and delete changes';

create or replace table CONSUMPTION_SCH.ORDER_ITEM_Fact (
    ORDER_ITEM_HK NUMBER PRIMARY KEY,             -- Hash key for the order item
    ORDER_ITEM_ID NUMBER NOT NULL,                -- Natural key for the order item
    ORDER_ID_FK NUMBER NOT NULL,                  -- Foreign key to order
    MENUITEM_ID_FK NUMBER NOT NULL,               -- Foreign key to menu item
    QUANTITY NUMBER(10, 2),                       -- Quantity ordered
    PRICE NUMBER(10, 2),                          -- Unit price
    SUBTOTAL NUMBER(10, 2),                       -- Subtotal (quantity * price)
    RATINGS NUMBER(10, 2),                        -- Rating given to the item
    EFF_START_DATE TIMESTAMP_TZ,                  -- Effective start date
    EFF_END_DATE TIMESTAMP_TZ,                    -- Effective end date (NULL if active)
    IS_CURRENT BOOLEAN                            -- Flag to indicate the current record
)
COMMENT = 'Order Item Dimension table with SCD Type 2 handling for historical tracking.';


select * from swiggy_db.stage_sch.orderitem;
select * from swiggy_db.stage_sch.orderitem_stm;

select * from swiggy_db.clean_sch.order_item;
select * from swiggy_db.clean_sch.order_item_stm;

select * from swiggy_db.consumption_sch.order_item_fact;


create or replace procedure swiggy_db.common.ORDER_ITEM_MAIN_PROCEDURE(stage_name STRING)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    merge_statement VARCHAR;
    result_set RESULTSET;
BEGIN

     -- first copy into work 
     
        EXECUTE IMMEDIATE
                    ' copy into stage_sch.orderitem (orderitemid, orderid, menuitemid, quantity, price, 
                                 subtotal, ratings, createddate, modifieddate,
                                 _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
                        from (
                            select 
                                t.$1::text as orderitemid,
                                t.$2::text as orderid,
                                t.$3::text as menuitemid,
                                t.$4::text as quantity,
                                t.$5::text as price,
                                t.$6::text as subtotal,
                                t.$7::text as ratings,
                                t.$8::text as createddate,
                                t.$9::text as modifieddate,
                                metadata$filename as _stg_file_name,
                                metadata$file_last_modified as _stg_file_load_ts,
                                metadata$file_content_key as _stg_file_md5,
                                current_timestamp as _copy_data_ts
                            from  ' || stage_name || ' t
                        )
                        file_format = (format_name = ''swiggy_db.STAGE_SCH.CSV_FILE_FORMAT'')
                        on_error = abort_statement; ';
         

        -- second merge into clean sch menu 
        
        MERGE INTO clean_sch.order_item AS target
USING stage_sch.orderitem_stm AS source
ON  
    target.order_item_id = source.orderitemid and
    target.order_id_fk = source.orderid and
    target.menuitem_id_fk = source.menuitemid
WHEN MATCHED and (
        target.quantity != source.quantity OR
        target.price != source.price OR
        target.subtotal != source.subtotal OR
        not equal_null(target.RATINGS , source.RATINGS)
) THEN
    -- Update the existing record with new data
    UPDATE SET 
        target.quantity = source.quantity,
        target.price = source.price,
        target.subtotal = source.subtotal,
        target.ratings = source.ratings,
        target.modified_dt = source.modifieddate,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new record if no match is found
    INSERT (
        order_item_id,
        order_id_fk,
        menuitem_id_fk,
        quantity,
        price,
        subtotal,
        ratings,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        source.orderitemid,
        source.orderid,
        source.menuitemid,
        source.quantity,
        source.price,
        source.subtotal,
        source.ratings,
        source.createddate,
        source.modifieddate,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP()
    );


    
       result_set := ( -- MERGE statement to load data from clean layer to consumption layer with SCD Type 2 handling
MERGE INTO 
    CONSUMPTION_SCH.ORDER_ITEM_FACT AS target
USING 
    CLEAN_SCH.ORDER_ITEM_STM AS source
ON 
    target.ORDER_ITEM_ID = source.ORDER_ITEM_ID AND
    target.ORDER_ID_FK = source.ORDER_ID_FK AND
    target.MENUITEM_ID_FK = source.MENUITEM_ID_FK AND
    target.QUANTITY = source.QUANTITY AND
    target.PRICE = source.PRICE AND
    target.SUBTOTAL = source.SUBTOTAL AND
    equal_null(target.RATINGS , source.RATINGS) AND
    target.IS_CURRENT = TRUE
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
        ORDER_ITEM_HK,
        ORDER_ITEM_ID,
        ORDER_ID_FK,
        MENUITEM_ID_FK,
        QUANTITY,
        PRICE,
        SUBTOTAL,
        RATINGS,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.ORDER_ITEM_ID, source.ORDER_ID_FK, source.MENUITEM_ID_FK, 
            source.QUANTITY, source.PRICE, source.SUBTOTAL, source.RATINGS))),
        source.ORDER_ITEM_ID,
        source.ORDER_ID_FK,
        source.MENUITEM_ID_FK,
        source.QUANTITY,
        source.PRICE,
        source.SUBTOTAL,
        source.RATINGS,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        ORDER_ITEM_HK,
        ORDER_ITEM_ID,
        ORDER_ID_FK,
        MENUITEM_ID_FK,
        QUANTITY,
        PRICE,
        SUBTOTAL,
        RATINGS,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.ORDER_ITEM_ID, source.ORDER_ID_FK, source.MENUITEM_ID_FK, 
            source.QUANTITY, source.PRICE, source.SUBTOTAL, source.RATINGS))),
        source.ORDER_ITEM_ID,
        source.ORDER_ID_FK,
        source.MENUITEM_ID_FK,
        source.QUANTITY,
        source.PRICE,
        source.SUBTOTAL,
        source.RATINGS,
        source.CREATED_DT,
        NULL,
        TRUE
    )   );
        
        RETURN TABLE(result_set);
    
    END;
$$;

-- CALL SWIGGY_DB.COMMON.ORDER_ITEM_MAIN_PROCEDURE('@STAGE_SCH.AWS_S3_STAGE/2025/4/9/order_items.csv');
