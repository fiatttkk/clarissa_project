# Data pipeline end-to-end project [Fashion retail shop]

Hi! I'm Fiat and this is my first data engineering project.
Full version on Medium :
[# Fashion retail shop; Data pipeline end-to-end Project [Medium]](https://medium.com/@fiat.ttkk/fashion-retail-shop-data-pipeline-end-to-end-project-data-engineer-cb01e066049c)
(SQLs are on medium)

## What I did

- Profiling data with pandas (Jupyternotebook)
- Wrangling test data with pandas (Jupyternotebook)
- Apply to Apache airflow dags (Python)
- Using rest API (Python, json)
- Design table on Data warehouse (Bigquery SQL)
- Create Data visualization (Lookerstudio UI)

## Technology stack I chose
- Apache airflow
- Postgresql
- Pgadmin
- Google cloud storage
- Google Bigquery
- Looker studio

# ETL method
Full load
![Alt text](images/clarissa_pipeline(GCP).png)

# Start
## Mount volume

```
volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/import_data:/home/airflow/import_data
    - ${AIRFLOW_PROJ_DIR:-.}/data:/home/airflow/export_data
    - ${AIRFLOW_PROJ_DIR:-.}/credentials:/home/airflow/credentials
```

## Dockerfile

```
FROM apache/airflow:2.6.2
USER root
RUN apt-get update && apt-get install -y libgeos-dev
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
```

## Requirements (Dependencies)

```
pandas>=1.3.5
SQLAlchemy>=1.4.0
psycopg2-binary==2.9.6
google.cloud==0.34.0
gspread==5.10.0
google-cloud-storage
regex
requests
```

## To install dependencies

`docker-compose build`

## To run this container as a service (first time)

`docker-compose up -d`

## To stop the container

`docker-compose stop`

## To run the container after ran first time

`docker-compose run`

## ** See all wrangling, transferring data code & method in dags folder **

## Monitoring on webserver

http://localhost:8080/

# Create table on Data warehouse (After running airflow, SQL languages)

## All data table

```
DROP VIEW IF EXISTS `all_data`;
CREATE VIEW `all_data` AS
SELECT
Order_ID,
Cancelation_or_Return_Type,
Seller_SKU,
Product_Name,
Variation,
Quantity,
SKU_Unit_Original_Price,
SKU_Subtotal_After_Discount,
SKU_Seller_Discount,
SKU_Subtotal_After_Discount -
Shipping_Fee_After_Discount -
Cost -
Transaction_Fee -
Tiktok_Shop_Commission_Fee -
Affiliate_Commission - 
Affiliate_Partner_Commission -
Charge_Back -
Customer_Service_Compensation -
Other_Adjustments -
Deductions_incurred_by_Seller -
Promotion_adjustment -
Satisfaction_reimbursement
AS Profit,
Affiliate_Commission + 
Affiliate_Partner_Commission
AS Affiliate_commission,
Transaction_Fee +
Tiktok_Shop_Commission_Fee +
Charge_Back +
Customer_Service_Compensation +
Other_Adjustments +
Deductions_incurred_by_Seller +
Promotion_adjustment +
Satisfaction_reimbursement
AS Tiktok_fees,
Created_Time,
Buyer_Username,
Country,
Province,
District,
Payment_Method
FROM `main_table`
ORDER BY Created_Time
```

## All cancelled data table

```
DROP VIEW IF EXISTS `all_cancelled_data_table`;
CREATE VIEW `all_cancelled_data_table` AS
SELECT Order_ID, Seller_SKU,
Product_Name,
Variation,
Quantity,
SKU_Unit_Original_Price,
SKU_Subtotal_After_Discount,
SKU_Seller_Discount,
SKU_Subtotal_After_Discount -
Shipping_Fee_After_Discount -
Cost -
Transaction_Fee -
Tiktok_Shop_Commission_Fee -
Affiliate_Commission - 
Affiliate_Partner_Commission -
Charge_Back -
Customer_Service_Compensation -
Other_Adjustments -
Deductions_incurred_by_Seller -
Promotion_adjustment -
Satisfaction_reimbursement
AS Profit,
Affiliate_Commission + 
Affiliate_Partner_Commission
AS Affiliate_commission,
Transaction_Fee +
Tiktok_Shop_Commission_Fee +
Charge_Back +
Customer_Service_Compensation +
Other_Adjustments +
Deductions_incurred_by_Seller +
Promotion_adjustment +
Satisfaction_reimbursement
AS Tiktok_fees,
Created_Time,
Buyer_Username,
Country,
Province,
District,
Payment_Method
FROM `main_table`
WHERE Cancelation_or_Return_Type IS NOT NULL
ORDER BY Created_Time
```

## All data exclude cancelled table

```
DROP VIEW IF EXISTS `all_data_exc_cancelled`;
CREATE VIEW `all_data_exc_cancelled` AS
SELECT
Order_ID,
Seller_SKU,
Product_Name,
Variation,
Quantity,
SKU_Unit_Original_Price,
SKU_Subtotal_After_Discount,
SKU_Seller_Discount,
SKU_Subtotal_After_Discount -
Shipping_Fee_After_Discount -
Cost -
Transaction_Fee -
Tiktok_Shop_Commission_Fee -
Affiliate_Commission - 
Affiliate_Partner_Commission -
Charge_Back -
Customer_Service_Compensation -
Other_Adjustments -
Deductions_incurred_by_Seller -
Promotion_adjustment -
Satisfaction_reimbursement
AS Profit,
Affiliate_Commission + 
Affiliate_Partner_Commission
AS Affiliate_commission,
Transaction_Fee +
Tiktok_Shop_Commission_Fee +
Charge_Back +
Customer_Service_Compensation +
Other_Adjustments +
Deductions_incurred_by_Seller +
Promotion_adjustment +
Satisfaction_reimbursement
AS Tiktok_fees,
Created_Time,
Buyer_Username,
Country,
Province,
District,
Payment_Method
FROM `main_table`
WHERE Cancelation_or_Return_Type IS NULL
ORDER BY Created_Time
```

## Customer status table

```
DROP VIEW IF EXISTS `customer_status`;
CREATE VIEW `customer_status` AS
WITH months AS
(
  SELECT DISTINCT
    EXTRACT(YEAR FROM Created_Time) AS Year,
    EXTRACT(MONTH FROM Created_Time) AS Month_number,
    FORMAT_DATE('%Y%m', Created_Time) AS Year_Month,
    FORMAT_DATE('%B', Created_Time) AS Month,
    COUNT(DISTINCT Buyer_Username) AS Total_Customer
  from `main_table`
  GROUP BY Year, Month_number, Month, Year_Month
  ORDER BY Year, Month_number
),
first_time_buyers AS
(
  SELECT MIN(created_time) AS first_datetime,
  Buyer_Username
  from `main_table`
  GROUP BY Buyer_Username
)
SELECT
  PARSE_DATE("%Y%m", Year_Month) AS Year_month,
  m.Total_customer,
  (
    SELECT COUNT(Buyer_Username)
    FROM first_time_buyers
    WHERE EXTRACT(YEAR FROM first_datetime) = m.Year
    AND EXTRACT(MONTH FROM first_datetime) = m.Month_number
    AND Buyer_Username IS NOT NULL
  ) AS New_customers,
  m.Total_customer -
  (
    SELECT COUNT(Buyer_Username)
    FROM first_time_buyers
    WHERE EXTRACT(YEAR FROM first_datetime) = m.Year
    AND EXTRACT(MONTH FROM first_datetime) = m.Month_number
    AND Buyer_Username IS NOT NULL
  ) AS Old_customers
FROM months m
ORDER BY m.Year, m.Month_number
```
That's all

# Credit
- Teetat Kitsakul (Project Owner)
- github.com/kongvut (Rest API owner)