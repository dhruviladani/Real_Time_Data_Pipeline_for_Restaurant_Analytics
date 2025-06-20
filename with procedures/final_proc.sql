create or replace procedure swiggy_db.common.FINAL_PROCEDURE(stage_name string)
returns string
LANGUAGE SQL
as
$$
DECLARE 
    location_csv string;
    restaurant_csv string;
    menu_items_csv string;
    orders_csv string;
    order_items_csv string;
    delivery_csv string;
    delivery_agent_json string;
    customer_csv string;
    customer_address_csv string;
    login_audit_csv string;
BEGIN

    location_csv := stage_name || 'location.csv';
    restaurant_csv := stage_name || 'restaurant.csv';
    menu_items_csv := stage_name || 'menu_items.csv';
    orders_csv := stage_name || 'orders.csv';
    order_items_csv := stage_name || 'order_items.csv';
    delivery_csv := stage_name || 'delivery.csv';
    delivery_agent_json := stage_name || 'delivery_agent.json';
    customer_csv := stage_name || 'customer.csv';
    customer_address_csv := stage_name || 'customer_address.csv';
    login_audit_csv := stage_name || 'login_audit.csv';
    
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.LOCATION_MAIN_PROCEDURE('|| location_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.RESTAURANT_MAIN_PROCEDURE('|| restaurant_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.MENU_MAIN_PROCEDURE('|| menu_items_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.ORDERS_MAIN_PROCEDURE('|| orders_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.ORDER_ITEM_MAIN_PROCEDURE('|| order_items_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.DELIVERY_MAIN_PROCEDURE('|| delivery_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.DELIVERY_AGENT_MAIN_PROCEDURE('|| delivery_agent_json ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.CUSTOMER_MAIN_PROCEDURE('|| customer_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.CUSTOMER_ADDRESS_MAIN_PROCEDURE('|| customer_address_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.LOGIN_AUDIT_MAIN_PROCEDURE('|| login_audit_csv ||')';

    RETURN 'ALL PROCEDURES EXECUTED';
END;
$$;

-- json file format for delivery agent data and csv for other datas
-- CALL SWIGGY_DB.COMMON.FINAL_PROCEDURE('@STAGE_SCH.AWS_S3_STAGE/2025/4/27/');


select * from consumption_sch.customer_dim;
select * from consumption_sch.customer_address_dim ;
select * from consumption_sch.location_dim;
select * from consumption_sch.delivery_dim ;
select * from consumption_sch.menu_dim;
select * from consumption_sch.order_item_fact;
select * from consumption_sch.orders_fact;
select * from consumption_sch.restaurant_dim;
select * from consumption_sch.delivery_agent_dim;
select * from consumption_sch.login_audit_fact;





select menuitem_id_fk,count(menuitem_id_fk)  from consumption_sch.order_item_fact where EFF_START_DATE > '2025-04-25' group by menuitem_id_fk order by count(menuitem_id_fk) desc ;

select * from consumption_sch.menu_dim
where menu_ID ='7609' or ITEM_NAME ='Gobi Manchurian';



select m.ITEM_NAME ,count(m.ITEM_NAME) from 
consumption_sch.order_item_fact  as i
join consumption_sch.menu_dim as m
on i.menuitem_id_fk = m.menu_id
group by m.ITEM_NAME
;


-- select restaurant_id , count(restaurant_id) from swiggy_db.consumption_sch.restaurant_dim group by
-- restaurant_id order by count(restaurant_id) desc;

-- select menu_id , count(menu_id) from swiggy_db.consumption_sch.menu_dim group by
-- menu_id order by count(menu_id) desc;
