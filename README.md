# Real-Time Data Pipeline for Restaurant Analytics 📊

## 📌 Overview
This project showcases the implementation of a real-time data engineering pipeline using modern tools and technologies. The aim is to collect, store, process, and visualize data for restaurant delivery and performance analytics using cloud-native services.

## 🚀 Problem Statement
The rapid growth of food delivery platforms has created a massive influx of unstructured and semi-structured data. Restaurants often lack proper infrastructure to analyze performance metrics like top-selling items, delivery efficiency, and operational effectiveness.

Our goal is to:

1. Store data in S3 with proper folder structures.
2. Load into Snowflake using SCD1, SCD2, and CDC strategies.
3. Visualize insights in Power BI.

## 🛠️ Tech Stack

| Tool/Tech        | Role                                                        |
|------------------|-------------------------------------------------------------|
| Python           | For Data Generation                                         |
| SQL              | For transformations and data modeling                       |
| Snowflake        | Cloud data warehouse for storing and querying data          |
| AWS S3           | Data lake to store raw and processed files                  |
| Power BI         | Dashboard for insights and KPIs                             |
| Git/GitHub       | Version control and collaboration                           |
| Pandas           | Data Manipulation                                           |
| Jupyter Notebook | Interactive Data Analysis                                   |

## 📁 Folder Structure

```
Internship_project/
├── data_generation/
│   └── main/
│       ├── Data Generation
│       ├── Uploading data into S3
│       └── AWS authentication credentials
├── snowflake-code/
│   ├── customer table code
│   ├── customer_address code
│   ├── delivery table code
│   ├── delivery agent code
│   ├── location table code
│   ├── menu table code
│   ├── orders table code
│   ├── order_items table code
│   └── restaurant table code
├── snowflake-code-v2/
│   └── sql_Scripts - creation of database, schema, file_format
├── snowflake-code-v3/
│   └── procedure_Scripts/
│       ├── customer_address_proc
│       ├── customer_proc
│       ├── delivery_agent_proc
│       ├── delivery_proc
│       ├── location_proc
│       ├── login_audit_proc
│       ├── menu_proc
│       ├── order_item_proc
│       ├── order_proc
│       ├── restaurant_proc
│       └── final_proc
├── Swiggy_Report_2025-contains the final dashboard
```

## 🔄 Project Workflow

- ✅ Step 1: Generate Synthetic Data using `data_generation/Main_DG` and store it as CSV.
- 🧹 Step 2: Dump the data into S3 using `s3_upload.py` and authenticate with `.env.local`.
- 📤 Step 3: Load Data into Snowflake external stage using SQL scripts (`snowflake-code-v2`).
- 📤 Step 4: Create tables and procedures using `snowflake-code-v3/procedure_scripts/`.
- 📊 Step 5: Connect Power BI Desktop with Snowflake and build dashboards.

## 🔍 Features

- 🔧 Modular design for generation, transformation, and loading
- 🏗 Snowflake integration with reusable utility functions
- 📈 Ready-to-analyze datasets with realistic food delivery platform structure
- 📊 Interactive data exploration in notebooks

## ✅ Use Cases

- Simulate large-scale data pipelines for food delivery systems
- Build BI dashboards using Snowflake data
- Practice advanced SQL and data modeling
- Apply Slowly Changing Dimensions (SCD) or Change Data Capture (CDC)

## 📊 Power BI Report

Access here: [Swiggy_Report_2025.pbix](https://github.com/Falsi3007/Internship_project/blob/main/Swiggy_Report_2025.pbix)

## 📋 Tables and Column Names 

| Table           | Column Names |
|----------------|--------------|
| customers       | CustomerID, Full_Name, Email, Mobile_no, LoginByUsing, Gender, DOB, Anniversary, Rating, Preferences, CreatedDate, ModifiedDate |
| Customer_Address| AddressID, CustomerID, FlatNo/HouseNo, Floor, Building, Landmark, Locality, City, State, PinCode, Coordinates, PrimaryFlag, AddressType, CreatedDate, ModifiedDate |
| Delivery        | DeliveryID, OrderID, DeliveryAgentID, DeliveryStatus, EstimatedTime, DeliveredTime, AddressID, DeliveryDate, CreatedDate, ModifiedDate |
| Delivery_Agent  | DeliveryAgentID, Full_Name, Email, Mobile_no, VehicleType, LocationID, Status, Gender, Rating, CreatedDate, ModifiedDate |
| Location        | LocationID, City, State, PinCode, ActiveFlag, CreatedDate, ModifiedDate |
| Login_Audit     | LoginID, CustomerID, LoginType, DeviceInterface, MobileDeviceName, WebInterface, LastLogin |
| Menu_Items      | MenuItemID, RestaurantID, ItemName, Description, Price, Category, Availability, ItemType, Ratings, CreatedDate, ModifiedDate |
| Order_items     | OrderItemID, OrderID, MenuItemID, Quantity, Price, Subtotal, Ratings, CreatedDate, ModifiedDate |
| Orders          | OrderID, CustomerID, RestaurantID, OrderDate, TotalAmount, DiscountAmount, DeliveryCharges, FinalAmount, Status, PaymentMethod, IsFirstOrder, CouponApplied, CouponCode, CreatedDate, ModifiedDate |
| Restaurant      | RestaurantID, Name, CuisineType, Pricing_for_2, Restaurant_Phone, OperatingHours, LocationID, ActiveFlag, OpenStatus, Locality, Restaurant_Address, Ratings, Coupons, Latitude, Longitude, CreatedDate, ModifiedDate |

🔗 ER Diagram: [dbdiagram.io](https://dbdiagram.io/d/internship_project-67acceb1263d6cf9a0ef3a03)

## 📈 KPIs Tracked

1. Total Revenue generated
2. Average Order Value (AOV)
3. Total Customers
4. Total Cities of Operation
5. Top Performing City
6. Total Orders
7. Number of Delivery Agents
8. Total Restaurants
9. Average Restaurant Rating
10. Returned Amount Rate
11. Returned Deliveries
12. Churn Rate (3 months)
13. Retention Rate
14. Payment Method Distribution
15. Most Valuable Customer
16. Revenue Growth (% Yearly)
17. Order Cancellation Rate
18. Revenue per Restaurant
19. Revenue per State
20. Revenue per Order Item
21. Avg. Successful Deliveries per Agent
22. Average Delivery Time
23. Avg. Customer Waiting Time
24. Avg. Delivery Partner Rating
25. Deliveries per Hour (Distribution)
26. Delivery Status Rate (Success, Failed, Returned)

## 📚 References

- AWS S3 to Snowflake Integration: [Medium Article](https://snowflakewiki.medium.com/connecting-snowflake-to-aws-ef7b6de1d6aa)
