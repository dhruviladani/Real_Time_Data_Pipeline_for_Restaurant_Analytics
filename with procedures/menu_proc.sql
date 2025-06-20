use role sysadmin;
use database swiggy_db;
use schema stage_sch;
use warehouse adhoc_wh;


create or replace table swiggy_db.stage_sch.menu (
    menu_id text comment 'Primary Key (Source System)',                   -- primary key as text
    restaurant_id text comment 'Restaurant FK(Source System)',             -- foreign key reference as text (no constraint in snowflake)
    item_name text,                 -- item name as text
    description text,              -- description as text
    price text,                    -- price as text (no decimal constraint)
    category text,                 -- category as text
    availability text,             -- availability as text
    item_type text,                 -- item type as text
    rating text,                   -- Ratign of every item
    created_date text,              -- created date as text
    modified_date text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the menu stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream swiggy_db.stage_sch.menu_stm 
on table swiggy_db.stage_sch.menu
append_only = true
comment = 'This is the append-only stream object on menu entity that only gets delta data';

create or replace table swiggy_db.clean_sch.menu (
    Menu_SK INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)',  -- Auto-incrementing primary key for internal tracking
    Menu_ID INT NOT NULL UNIQUE comment 'Primary Key (Source System)' ,             -- Unique and non-null Menu_ID
    Restaurant_ID_FK INT comment 'Restaurant FK(Source System)' ,                      -- Identifier for the restaurant
    Item_Name STRING not null,                        -- Name of the menu item
    Description STRING not null,                     -- Description of the menu item
    Price DECIMAL(10, 2) not null,                   -- Price as a numeric value with 2 decimal places
    Category STRING,                        -- Food category (e.g., North Indian)
    Availability BOOLEAN,                   -- Availability status (True/False)
    Item_Type STRING,                        -- Dietary classification (e.g., Veg)
    Rating DECIMAL(2, 1),                   --Rating of items out of 5
    Created_date TIMESTAMP_TZ,               -- Date when the record was created
    Modified_date TIMESTAMP_TZ,              -- Date when the record was last modified

    -- Audit columns for traceability
    _STG_FILE_NAME STRING,                  -- Source file name
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,        -- Timestamp when data was loaded from the staging layer
    _STG_FILE_MD5 STRING,                   -- MD5 hash of the source file
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP -- Timestamp when data was copied to the clean layer
)
comment = 'Menu entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream CLEAN_SCH.menu_stm 
on table CLEAN_SCH.menu
comment = 'This is the stream object on menu table table to track insert, update, and delete changes';

create or replace table consumption_sch.menu_dim (
    Menu_Dim_HK NUMBER primary key comment 'Menu Dim HK (EDW)',                         -- Hash key generated for Menu Dim table
    Menu_ID INT NOT NULL comment 'Primary Key (Source System)',                       -- Unique and non-null Menu_ID
    Restaurant_ID_FK INT NOT NULL comment 'Restaurant FK (Source System)',                          -- Identifier for the restaurant
    Item_Name STRING,                            -- Name of the menu item
    Description STRING,                         -- Description of the menu item
    Price DECIMAL(10, 2),                       -- Price as a numeric value with 2 decimal places
    Category STRING,                            -- Food category (e.g., North Indian)
    Availability BOOLEAN,                       -- Availability status (True/False)
    Item_Type STRING,                           -- Dietary classification (e.g., Vegan)
    Rating DECIMAL(2,1),                        -- Item ratings out of 5
    EFF_START_DATE TIMESTAMP_TZ,               -- Effective start date of the record
    EFF_END_DATE TIMESTAMP_TZ,                 -- Effective end date of the record
    IS_CURRENT BOOLEAN                         -- Flag to indicate if the record is current (True/False)
)
COMMENT = 'This table stores the dimension data for the menu items, tracking historical changes using SCD Type 2. Each menu item has an effective start and end date, with a flag indicating if it is the current record or historical. The hash key (Menu_Dim_HK) is generated based on Menu_ID and Restaurant_ID.';


select menu_id from swiggy_db.stage_sch.menu;
select * from swiggy_db.stage_sch.menu_stm;

select * from swiggy_db.clean_sch.menu;
select * from swiggy_db.clean_sch.menu_stm;

select * from swiggy_db.consumption_sch.menu_dim;


create or replace procedure swiggy_db.common.MENU_MAIN_PROCEDURE(stage_name STRING)
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
        '
        copy into stage_sch.menu (menu_id, restaurant_id, item_name, description, price, category, 
                availability, item_type, rating, created_date, modified_date,
                _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
                from (
                    select 
                        t.$1::text as menu_id,
                        t.$2::text as restaurant_id,
                        t.$3::text as item_name,
                        t.$4::text as description,
                        t.$5::text as price,
                        t.$6::text as category,
                        t.$7::text as availability,
                        t.$8::text as item_type,
                        t.$9::text as rating,
                        t.$10::text as created_date,
                        t.$11::text as modified_date,
                        metadata$filename as _stg_file_name,
                        metadata$file_last_modified as _stg_file_load_ts,
                        metadata$file_content_key as _stg_file_md5,
                        current_timestamp as _copy_data_ts
                    from ' || stage_name ||' as t
                )
                file_format = (format_name = ''stage_sch.csv_file_format'')
                on_error = abort_statement;
                ';
         

        -- second merge into clean sch menu 
        
        MERGE INTO clean_sch.menu AS target
    USING (
    SELECT 
        TRY_CAST(menu_id AS INT) AS Menu_ID,
        TRY_CAST(restaurant_id AS INT) AS Restaurant_ID_FK,
        TRIM(item_name) AS Item_Name,
        TRIM(description) AS Description,
        TRY_CAST(price AS DECIMAL(10, 2)) AS Price,
        TRIM(category) AS Category,
        CASE 
            WHEN LOWER(availability) = 'true' THEN TRUE
            WHEN LOWER(availability) = 'false' THEN FALSE
            ELSE NULL
        END AS Availability,
        TRIM(item_type) AS Item_Type,
        TRY_CAST(Rating as DECIMAL(2,1)) AS Rating,
        TRY_CAST(created_date AS TIMESTAMP_TZ) AS Created_date,  -- Renamed column
        TRY_CAST(modified_date AS TIMESTAMP_TZ) AS Modified_date, -- Renamed column
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM stage_sch.menu_stm
) AS source
ON target.Menu_ID = source.Menu_ID
WHEN MATCHED AND (
                target.Description != source.Description OR
                target.Price != source.Price OR
                target.Availability != source.Availability OR
                not equal_null(target.rating, source.rating)
            ) THEN
    UPDATE SET
        Description = source.Description,
        Price = source.Price,
        Availability = source.Availability,
        Rating = source.Rating,
        Created_date = source.Created_date,  
        _STG_FILE_NAME = source._stg_file_name,
        _STG_FILE_LOAD_TS = source._stg_file_load_ts,
        _STG_FILE_MD5 = source._stg_file_md5,
        _COPY_DATA_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        Rating,
        Created_date, 
        Modified_date,  
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    )
    VALUES (
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        source.Rating,
        source.Created_date,  
        source.Modified_date,  
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP
    );

    
   
       result_set := (MERGE INTO 
    swiggy_db.consumption_sch.MENU_DIM AS target
USING 
    CLEAN_SCH.MENU_STM AS source
ON 
    target.IS_CURRENT = TRUE AND
    target.Menu_ID = source.Menu_ID AND
    target.Restaurant_ID_FK = source.Restaurant_ID_FK AND
    target.Item_Name = source.Item_Name AND
    target.Description = source.Description AND
    target.Price = source.Price AND
    target.Category = source.Category AND
    target.Availability = source.Availability AND
    target.Item_Type = source.Item_Type AND
    equal_null(target.rating, source.rating) 
WHEN MATCHED 
    AND source.METADATA$ACTION = 'DELETE' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        Menu_Dim_HK,               -- Hash key
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        Rating,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, 
            source.Item_Name, source.Description, source.Price, 
            source.Category, source.Availability, source.Item_Type))),  -- Hash key
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        source.Rating,
        CURRENT_TIMESTAMP(),       -- Effective start date
        NULL,                      -- Effective end date (NULL for current record)
        TRUE                       -- IS_CURRENT = TRUE for new record
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        Menu_Dim_HK,               -- Hash key
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        Rating,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, 
            source.Item_Name, source.Description, source.Price, 
            source.Category, source.Availability, source.Item_Type))),  -- Hash key
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        source.Rating,
        Created_date,                -- Effective start date
        NULL,                      -- Effective end date (NULL for current record)
        TRUE                       -- IS_CURRENT = TRUE for new record
    ));
        
         RETURN TABLE(result_set);
    
    END;
$$;

-- CALL SWIGGY_DB.COMMON.MENU_MAIN_PROCEDURE('@STAGE_SCH.AWS_S3_STAGE/2025/4/9/menu_items.csv');

select menu_id from SWIGGY_DB.CONSUMPTION_SCH.MENU_DIM group by menu_id having count(*) > 1;

select * from swiggy_db.consumption_sch.menu_dim;
