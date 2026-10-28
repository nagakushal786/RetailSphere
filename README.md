# RetailSphere: End-to-End Retail Analytics with Databricks

An end-to-end retail analytics project built with **Databricks, PySpark,
Delta Lake, and SQL**, using the **Medallion Architecture** to transform
raw customer, product, and sales data into curated analytical tables and
a sales dashboard.

The project demonstrates data ingestion, data quality validation,
transformation, dimensional modeling, KPI development, and
business-focused reporting.

> **Project catalog:** `retailsphere`\
> **Architecture:** Bronze → Silver → Gold\
> **Dashboard data source:** `retailsphere.gold.vw_sales_dashboard`\
> **Main dashboard preparation script:** `complete_data.py`

------------------------------------------------------------------------

## Dashboard Preview

The dashboard summarizes sales performance, customer activity,
product/category contribution, payment preferences, order status, and
monthly sales patterns.

### 1. Sales Overview and Trends

![Sales overview: KPIs, monthly net sales trend, and revenue
distribution heatmap](images/dashboard-overview.png)

### 2. Payment Preferences and Top Products

![Payment preferences by distinct orders and top six products by net
sales](images/payment-and-products.png)

### 3. Category Sales and Order Status

![Net sales by category and order status
analysis](images/category-and-order-status.png)

------------------------------------------------------------------------

## Project Objectives

-   Build a repeatable data pipeline using Databricks and PySpark.
-   Organize raw and curated datasets using the Medallion Architecture.
-   Standardize inconsistent fields and validate customer, product, and
    sales records.
-   Create reusable Gold-layer fact and dimension tables for analytics.
-   Prepare a joined SQL view for dashboard reporting.
-   Identify commercial patterns and data-quality opportunities from the
    dashboard.

## Technology Stack

-   **Platform:** Databricks
-   **Processing:** Apache Spark / PySpark
-   **Storage format:** Delta Lake
-   **Querying:** Spark SQL
-   **Data architecture:** Medallion Architecture
-   **Reporting:** Dashboard built from the Gold-layer dashboard view
-   **Version control:** Git and GitHub

------------------------------------------------------------------------


## Dataset Details

The project uses three CSV datasets.

### 1. Customers — `customer_master_raw.csv`

| Column | Description |
|---|---|
| `customer_id` | Customer identifier |
| `customer_name` | Customer name |
| `email` | Customer email address |
| `signup_date` | Customer registration date |
| `loyalty_tier` | Loyalty tier, such as Bronze, Silver, Gold, or Platinum |
| `region` | Customer region |

### 2. Products — `product_master.csv`

| Column | Description |
|---|---|
| `product_id` | Product identifier |
| `product_name` | Product name |
| `category` | Product category |
| `unit_price` | Master product price |

### 3. Sales Transactions — `ecommerce_sales_raw.csv`

| Column | Description |
|---|---|
| `order_id` | Order identifier |
| `order_date` | Transaction date |
| `customer_id` | Customer identifier associated with the transaction |
| `product_id` | Product identifier |
| `product_name` | Product name recorded in the transaction |
| `category` | Transaction category |
| `quantity` | Quantity sold |
| `unit_price` | Transaction unit price |
| `discount_percent` | Discount percentage |
| `total_amount` | Recorded transaction amount |
| `city` | Order city |
| `payment_method` | Payment method |
| `order_status` | Order status |
| `customer_email` | Email recorded in the transaction source |
| `shipping_cost` | Shipping charge |


## Medallion Architecture

The pipeline separates raw ingestion, data cleaning, and business-ready
modeling into three layers.

``` text
CSV source files
      |
      v
Databricks Volume: retailsphere.source_data.raw_data
      |
      v
+------------------------------+
| BRONZE                       |
| Raw CSV data stored as Delta |
| brz_customers                |
| brz_products                 |
| brz_sales                    |
+------------------------------+
      |
      v
+------------------------------+
| SILVER                       |
| Standardized and validated   |
| slv_customers                |
| slv_products                 |
| slv_sales                    |
+------------------------------+
      |
      v
+------------------------------+
| GOLD                         |
| Analytics-ready model        |
| dim_customers                |
| dim_products                 |
| fact_sales                   |
+------------------------------+
      |
      v
retailsphere.gold.vw_sales_dashboard
      |
      v
Sales dashboard and business analysis
```

### Bronze Layer: Raw Ingestion

**Script:** `1_bronze.py`

-   Reads customer, product, and sales CSV files from the Databricks Volume.
-   Applies explicit schemas to customer and product data, and initially
    ingests sales fields as strings during the detailed cleaning
    workflow.
-   Adds `_source_file` and `ingested_at` metadata to the ingested data.
-   Writes Delta tables:
    -   `retailsphere.bronze.brz_customers`
    -   `retailsphere.bronze.brz_products`
    -   `retailsphere.bronze.brz_sales`

### Silver Layer: Cleaning and Validation

**Script:** `2_silver.py`

The Silver layer standardizes data before it is used for reporting.

-   Trims whitespace and normalizes identifiers.
-   Validates customer IDs (`CUST` plus five digits), product IDs
    (`PROD` plus four digits), and order IDs (`ORD` plus six digits).
-   Cleans customer names and validates email formats.
-   Parses dates using multiple supported date formats.
-   Standardizes loyalty tiers, regions, product categories, payment
    methods, and order statuses.
-   Converts quantity, prices, discounts, transaction amounts, and
    shipping costs to appropriate numeric types.
-   Flags invalid or suspicious quantities, non-positive prices, invalid
    discounts, and invalid shipping charges.
-   Matches transaction customer/product keys to the master tables.
-   Uses the product master to standardize product attributes and to
    help resolve invalid or extreme transaction prices.
-   Reconciles recorded sales amounts against quantity, price, and
    discount calculations where possible.
-   Retains diagnostic fields during validation and writes the cleaned
    Silver tables.

Output tables:

-   `retailsphere.silver.slv_customers`
-   `retailsphere.silver.slv_products`
-   `retailsphere.silver.slv_sales`

### Gold Layer: Business Modeling

**Script:** `3_gold.py`

The Gold layer creates reusable analytical tables.

**`retailsphere.gold.dim_customers`** 
- Adds signup year, quarter,
month, and year-month fields.
- Calculates customer tenure and segments
customers by tenure.
- Ranks loyalty tiers. 
- Adds email availability
and profile-completeness indicators. 
- Excludes records without a valid
customer ID from the final dimension.

**`retailsphere.gold.dim_products`** 
- Groups products into price bands:
Budget, Mid-Range, Premium, and High-End. 
- Calculates category average
price and price variance relative to the category average. 
- Ranks
products by price within each category. 
- Adds product-name length and
category product-count attributes. 
- Excludes records without a valid
product ID from the final dimension.

**`retailsphere.gold.fact_sales`** 
- Adds calendar attributes such as
year, quarter, month, year-month, day of week, and weekend indicators. 
- Calculates gross sales, discount amount, net sales, and sales including
shipping where the required inputs are available.
- Creates discount
bands and order-status indicators. - Adds flags for reportable sales
records.

------------------------------------------------------------------------

## Dashboard Data Preparation

**Script:** `complete_data.py`

This script prepares and validates the dashboard's analytical source. It
does not contain the chart-rendering code itself; the visual dashboard
consumes the SQL view it creates.

**`Key Points`** 
- Reads the Gold fact and dimension tables.
- Checks for null or duplicate keys in the customer and product
    dimensions.
- Creates `retailsphere.gold.vw_sales_dashboard` by left-joining sales
    with customer and product details.
- Verifies that the joined view preserves the fact-table row count.
- Computes high-level metrics and monthly sales summaries for
    validation.

### Dashboard view

`retailsphere.gold.vw_sales_dashboard`

The view combines transaction, date, customer, product, payment,
location, order-status, and reporting fields in one reporting-friendly
dataset.

------------------------------------------------------------------------

## Dashboard KPIs and Visuals

The supplied dashboard images show these headline metrics:

| KPI | Displayed Value |
|---|---:|
| Net Sales | 1,952,274.6 |
| Total Orders | 1,200 |
| Unique Customers | 288 |
| Units Sold | 5,073 |

### Visuals included

-   **Net Sales Amount by Order Start Month:** Time-series view of
    monthly sales.
-   **Revenue Distribution:** Heatmap of sales by weekday and month.
-   **Payment Preferences by Distinct Orders:** Distribution across
    payment methods.
-   **Top 6 Products by Net Sales:** Highest-contributing products.
-   **Net Sales by Category:** Sales contribution across product
    categories.
-   **Order Status Analysis:** Counts across status values, including
    missing status.

------------------------------------------------------------------------

## Business Insights from the Dashboard

The following observations are based on the values visible in the
supplied screenshots. They are descriptive findings, not proof of the
causes behind the patterns.

### 1. Sales are concentrated in a small number of products

The six displayed top products are:

| Product | Displayed Net Sales |
|---|---:|
| Nail Polish | 285.15K |
| Children Story | 184.09K |
| Winter Jacket | 177.59K |
| Smartphone Case | 114.54K |
| Hair Oil | 92.41K |
| Face Cream | 67.61K |

**Business implication:** Nail Polish is the clear leader among the
displayed products. The business could review its stock availability,
replenishment planning, promotions, and margins. Children Story and
Winter Jacket are also strong contributors in this view. Sales alone do
not establish profitability, so margin and discount data should be
reviewed before prioritizing products.

### 2. Beauty is the largest sales category in the chart

Displayed net sales by category:

| Category | Displayed Net Sales |
|---|---:|
| Beauty | 547.7K |
| Clothing | 349.54K |
| Books | 319.24K |
| Electronics | 262.98K |
| Toys | 169.62K |
| Home & Kitchen | 154.86K |
| Sports | 148.35K |

**Business implication:** Beauty leads the displayed categories,
followed by Clothing and Books. Management could review inventory depth
and campaign performance in these categories while investigating whether
lower sales in Sports and Home & Kitchen reflect demand, product
availability, pricing, or marketing.

### 3. Monthly sales show substantial fluctuations

The monthly trend contains several pronounced peaks and many lower-sales
periods rather than a smooth progression.

**Business implication:** Investigate the high-sales months alongside
campaigns, seasonality, discounting, product availability, and
transaction volume. The chart alone does not establish why the peaks
occurred. Compare monthly net sales with order count and average order
value to separate volume effects from basket-value effects.

### 4. Payment preferences are spread across multiple methods

The donut chart displays these shares by distinct orders:

| Payment Method | Displayed Share |
|---|---:|
| Credit Card | 20.33% |
| PayPal | 16.75% |
| Missing (`null`) | 13.83% |
| UPI | 13.50% |
| Cash on Delivery | 13.42% |
| Cash | 7.67% |
| Debit Card | 7.58% |
| Net Banking | 6.92% |

**Business implication:** Credit Card is the largest displayed payment
segment, with PayPal next. UPI and Cash on Delivery also represent
meaningful shares. The missing payment-method segment is material and
should be investigated before drawing firm conclusions about customer
payment preferences. The business can use the cleaned data to evaluate
payment success, fees, returns, and conversion by method.

### 5. Missing order status is a major data-quality concern

The order-status chart shows:

| Status | Displayed Count |
|---|---:|
| Missing (`null`) | 213 |
| Pending | 208 |
| Shipped | 206 |
| Completed | 197 |
| Returned | 194 |
| Cancelled | 182 |

**Business implication:** Missing status is the largest displayed group.
Operations teams should investigate why these orders have no status and
whether source-system or pipeline changes can improve completeness.
Pending, shipped, completed, returned, and cancelled orders should be
analyzed separately to understand fulfilment and post-purchase outcomes.
Do not treat missing status as a valid business status.

### 6. Sales vary by weekday and month

The heatmap shows stronger cells for selected weekday/month combinations
rather than uniform sales throughout the calendar.

**Business implication:** Use these patterns as a starting point for
campaign timing, staffing, and inventory planning. Validate the pattern
over multiple periods and check the number of orders behind each cell
before making operational decisions.

------------------------------------------------------------------------

## Repository Structure

``` text
Databricks-Project/
├── 1_setup/
│   └── 1_setup.py
├── 2_medallion_processing/
│   ├── 1_bronze.py
│   ├── 2_silver.py
│   └── 3_gold.py
├── 3_final_view/
│   └── complete_data.py
├── datasets/
│   ├── customers/
│   │   └── customer_master_raw.csv
│   ├── products/
│   │   └── product_master.csv
│   └── sales/
│       └── ecommerce_sales_raw.csv
├── images/
│   ├── dashboard-overview.png
│   ├── payment-and-products.png
│   ├── category-and-order-status.png
│   └── project-structure.png
├── notes/
└── README.md
```

------------------------------------------------------------------------

## Skills Demonstrated

-   Databricks and Unity Catalog organization
-   PySpark data ingestion and transformation
-   Delta Lake table creation
-   Medallion Architecture
-   Data cleaning and validation
-   Date and numeric type standardization
-   Fact/dimension modeling
-   Window functions and derived business attributes
-   SQL views and join validation
-   KPI design and dashboard interpretation
-   Translating sales patterns into business recommendations

------------------------------------------------------------------------

## Conclusion

RetailSphere demonstrates an end-to-end workflow for turning raw retail
data into structured, analytics-ready datasets. The Medallion pipeline
separates ingestion, quality improvements, and business modeling, while
the Gold dashboard view brings sales, customer, and product information
together for reporting.

The dashboard points to clear follow-up opportunities: understand the
sales spikes, review high-contributing products and categories, and
address missing order-status and payment-method data before using the
analysis to guide operational decisions.
