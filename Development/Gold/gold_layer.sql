{% macro gold_layer() %}

-- ============================================================
-- GOLD LAYER
-- Catalog : wsdbrksecommerce
-- Schema  : gold
--
-- Tables:
--   1. dim_customer
--   2. dim_order
--   3. dim_product
--   4. dim_product_category
--   5. dim_seller
--   6. fact_order_items
--
-- Fact grain:
--   One row per order item
--
-- Purpose:
--   Complete dashboard-ready fact table
-- ============================================================


-- ============================================================
-- 1. DIM_CUSTOMER
-- ============================================================

{% set sql %}

CREATE OR REPLACE TABLE wsdbrksecommerce.gold.dim_customer AS

SELECT
    ROW_NUMBER() OVER (
        ORDER BY customer_id
    ) AS customer_key,

    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,

    INITCAP(TRIM(customer_city)) AS customer_city,

    UPPER(TRIM(customer_state)) AS customer_state,

    CURRENT_TIMESTAMP() AS gold_created_timestamp

FROM
(
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,

        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY customer_id
        ) AS rn

    FROM wsdbrksecommerce.silver.silver_customer_dataset

    WHERE customer_id IS NOT NULL
)

WHERE rn = 1

{% endset %}

{% do run_query(sql) %}


-- ============================================================
-- 2. DIM_PRODUCT_CATEGORY
-- ============================================================

{% set sql %}

CREATE OR REPLACE TABLE wsdbrksecommerce.gold.dim_product_category AS

WITH categories AS
(
    SELECT DISTINCT
        product_category_name

    FROM wsdbrksecommerce.silver.silver_products_dataset

    WHERE product_category_name IS NOT NULL
),

translations AS
(
    SELECT
        product_category_name,

        MAX(product_category_name_english)
            AS product_category_name_english

    FROM wsdbrksecommerce.silver.silver_product_category_name_translation

    GROUP BY product_category_name
)

SELECT

    ROW_NUMBER() OVER (
        ORDER BY c.product_category_name
    ) AS product_category_key,

    c.product_category_name,

    COALESCE(
        t.product_category_name_english,
        'unknown'
    ) AS product_category_name_english,

    CURRENT_TIMESTAMP() AS gold_created_timestamp

FROM categories c

LEFT JOIN translations t

    ON c.product_category_name =
       t.product_category_name

{% endset %}

{% do run_query(sql) %}


-- ============================================================
-- 3. DIM_PRODUCT
-- ============================================================

{% set sql %}

CREATE OR REPLACE TABLE wsdbrksecommerce.gold.dim_product AS

SELECT

    ROW_NUMBER() OVER (
        ORDER BY product_id
    ) AS product_key,

    product_id,

    product_category_key,

    product_category_name,

    product_category_name_english,

    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,

    CURRENT_TIMESTAMP() AS gold_created_timestamp

FROM
(
    SELECT

        p.product_id,

        pc.product_category_key,

        p.product_category_name,

        COALESCE(
            pc.product_category_name_english,
            'unknown'
        ) AS product_category_name_english,

        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,

        ROW_NUMBER() OVER (
            PARTITION BY p.product_id
            ORDER BY p.product_id
        ) AS rn

    FROM wsdbrksecommerce.silver.silver_products_dataset p

    LEFT JOIN
        wsdbrksecommerce.gold.dim_product_category pc

        ON p.product_category_name =
           pc.product_category_name

    WHERE p.product_id IS NOT NULL
)

WHERE rn = 1

{% endset %}

{% do run_query(sql) %}


-- ============================================================
-- 4. DIM_SELLER
-- ============================================================

{% set sql %}

CREATE OR REPLACE TABLE wsdbrksecommerce.gold.dim_seller AS

SELECT

    ROW_NUMBER() OVER (
        ORDER BY seller_id
    ) AS seller_key,

    seller_id,

    seller_zip_code_prefix,

    INITCAP(TRIM(seller_city)) AS seller_city,

    UPPER(TRIM(seller_state)) AS seller_state,

    CURRENT_TIMESTAMP() AS gold_created_timestamp

FROM
(
    SELECT

        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,

        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY seller_id
        ) AS rn

    FROM wsdbrksecommerce.silver.silver_sellers_dataset

    WHERE seller_id IS NOT NULL
)

WHERE rn = 1

{% endset %}

{% do run_query(sql) %}


-- ============================================================
-- 5. DIM_ORDER
-- ============================================================

{% set sql %}

CREATE OR REPLACE TABLE wsdbrksecommerce.gold.dim_order AS

SELECT

    ROW_NUMBER() OVER (
        ORDER BY order_id
    ) AS order_key,

    order_id,

    customer_id,

    order_status,

    order_business_status,

    order_purchase_timestamp,

    order_approved_at,

    order_delivered_carrier_date,

    order_delivered_customer_date,

    order_estimated_delivery_date,

    purchase_date,

    purchase_year,

    purchase_quarter,

    purchase_month,

    DAYOFWEEK(
        order_purchase_timestamp
    ) AS day_of_week,

    DATE_FORMAT(
        order_purchase_timestamp,
        'EEEE'
    ) AS day_name,

    WEEKOFYEAR(
        order_purchase_timestamp
    ) AS week_of_year,

    delivery_days,

    delivery_delay_days,

    delivery_status,

    CURRENT_TIMESTAMP() AS gold_created_timestamp

FROM
(
    SELECT

        order_id,
        customer_id,

        order_status,
        order_business_status,

        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        purchase_date,
        purchase_year,
        purchase_quarter,
        purchase_month,

        delivery_days,
        delivery_delay_days,
        delivery_status,

        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_purchase_timestamp
        ) AS rn

    FROM wsdbrksecommerce.silver.silver_orders_dataset

    WHERE order_id IS NOT NULL
)

WHERE rn = 1

{% endset %}

{% do run_query(sql) %}


-- ============================================================
-- 6. FACT_ORDER_ITEMS
-- ============================================================
-- ONE ROW = ONE ORDER ITEM
--
-- Contains exactly the dashboard fields requested:
--
-- KEYS
-- ORDER
-- CUSTOMER
-- PRODUCT
-- SELLER
-- DATE
-- TIMESTAMPS
-- SALES
-- DELIVERY
-- STATUS FLAGS
-- DELIVERY FLAGS
-- DATA QUALITY FLAGS
-- AUDIT
-- ============================================================

{% set sql %}

CREATE OR REPLACE TABLE wsdbrksecommerce.gold.fact_order_items AS

WITH customer_stats AS
(
    SELECT

        customer_unique_id,

        COUNT(DISTINCT order_id)
            AS customer_order_count,

        MIN(purchase_date)
            AS first_order_date,

        MAX(purchase_date)
            AS last_order_date

    FROM wsdbrksecommerce.silver.silver_sales

    WHERE customer_unique_id IS NOT NULL

    GROUP BY customer_unique_id
),

fact_data AS
(
    SELECT

        -- ====================================================
        -- KEYS
        -- ====================================================

        o.order_key,

        p.product_key,

        se.seller_key,

        c.customer_key,

        pc.product_category_key,


        -- ====================================================
        -- ORDER
        -- ====================================================

        s.order_id,

        s.order_item_id,

        s.order_status,

        s.order_business_status,


        -- ====================================================
        -- CUSTOMER
        -- ====================================================

        s.customer_id,

        s.customer_unique_id,

        c.customer_zip_code_prefix,

        COALESCE(
            c.customer_city,
            s.customer_city
        ) AS customer_city,

        COALESCE(
            c.customer_state,
            s.customer_state
        ) AS customer_state,


        -- ====================================================
        -- CUSTOMER RETENTION
        -- ====================================================

        cs.customer_order_count,

        cs.first_order_date,

        cs.last_order_date,

        CASE

            WHEN cs.customer_order_count = 1
                THEN 'One-Time Customer'

            WHEN cs.customer_order_count > 1
                THEN 'Repeat Customer'

            ELSE 'Unknown'

        END AS customer_segment,

        CASE

            WHEN s.purchase_date = cs.first_order_date
                THEN 1

            ELSE 0

        END AS first_order_flag,


        -- ====================================================
        -- PRODUCT
        -- ====================================================

        s.product_id,

        COALESCE(
            p.product_category_name,
            s.product_category_name
        ) AS product_category_name,

        COALESCE(
            p.product_category_name_english,
            s.product_category_name_english,
            'unknown'
        ) AS product_category_name_english,

        p.product_weight_g,

        p.product_length_cm,

        p.product_height_cm,

        p.product_width_cm,


        -- ====================================================
        -- SELLER
        -- ====================================================

        s.seller_id,

        se.seller_zip_code_prefix,

        COALESCE(
            se.seller_city,
            s.seller_city
        ) AS seller_city,

        COALESCE(
            se.seller_state,
            s.seller_state
        ) AS seller_state,


        -- ====================================================
        -- DATE
        -- ====================================================

        s.purchase_date,

        s.purchase_year,

        s.purchase_quarter,

        s.purchase_month,

        DAYOFWEEK(
            s.order_purchase_timestamp
        ) AS day_of_week,

        DATE_FORMAT(
            s.order_purchase_timestamp,
            'EEEE'
        ) AS day_name,

        WEEKOFYEAR(
            s.order_purchase_timestamp
        ) AS week_of_year,

        DATE_FORMAT(
            s.order_purchase_timestamp,
            'yyyy-MM'
        ) AS year_month,

        DATE_FORMAT(
            s.order_purchase_timestamp,
            'MMMM'
        ) AS month_name,


        -- ====================================================
        -- TIMESTAMPS
        -- ====================================================

        s.order_purchase_timestamp,

        s.order_approved_at,

        s.order_delivered_carrier_date,

        s.order_delivered_customer_date,

        s.order_estimated_delivery_date,


        -- ====================================================
        -- SALES
        -- ====================================================

        CAST(
            COALESCE(s.price, 0)
            AS DECIMAL(18,2)
        ) AS price,

        CAST(
            COALESCE(s.freight_value, 0)
            AS DECIMAL(18,2)
        ) AS freight_value,

        CAST(
            COALESCE(s.item_quantity, 1)
            AS INT
        ) AS quantity,

        CAST(
            COALESCE(s.total_item_value, 0)
            AS DECIMAL(18,2)
        ) AS total_item_value,


        -- ====================================================
        -- DELIVERY
        -- ====================================================

        s.delivery_days,

        s.delivery_delay_days,

        s.delivery_status,


        -- ====================================================
        -- STATUS FLAGS
        -- ====================================================

        CASE

            WHEN LOWER(s.order_status) = 'delivered'
                THEN 1

            ELSE 0

        END AS delivered_flag,


        CASE

            WHEN LOWER(s.order_status)
                 IN ('canceled', 'cancelled')
                THEN 1

            ELSE 0

        END AS cancelled_flag,


        CASE

            WHEN LOWER(s.order_status) = 'unavailable'
                THEN 1

            ELSE 0

        END AS unavailable_flag,


        -- ====================================================
        -- DELIVERY FLAGS
        -- ====================================================

        CASE

            WHEN LOWER(s.delivery_status) = 'late'
                THEN 1

            ELSE 0

        END AS late_delivery_flag,


        CASE

            WHEN LOWER(s.delivery_status) = 'on_time'
                THEN 1

            ELSE 0

        END AS on_time_delivery_flag,


        -- ====================================================
        -- DATA QUALITY FLAGS
        -- ====================================================

        CASE

            WHEN s.price IS NULL
                 OR s.price < 0
                THEN 1

            ELSE 0

        END AS invalid_price_flag,


        CASE

            WHEN s.freight_value IS NULL
                 OR s.freight_value < 0
                THEN 1

            ELSE 0

        END AS invalid_freight_flag,


        CASE

            WHEN s.item_quantity IS NULL
                 OR s.item_quantity <= 0
                THEN 1

            ELSE 0

        END AS invalid_quantity_flag,


        CASE

            WHEN s.customer_id IS NULL
                THEN 1

            ELSE 0

        END AS missing_customer_flag,


        CASE

            WHEN s.product_id IS NULL
                THEN 1

            ELSE 0

        END AS missing_product_flag,


        CASE

            WHEN s.seller_id IS NULL
                THEN 1

            ELSE 0

        END AS missing_seller_flag,


        CASE

            WHEN s.product_category_name IS NULL
                THEN 1

            ELSE 0

        END AS missing_category_flag


    FROM wsdbrksecommerce.silver.silver_sales s


    -- ========================================================
    -- CUSTOMER
    -- ========================================================

    LEFT JOIN wsdbrksecommerce.gold.dim_customer c

        ON s.customer_id =
           c.customer_id


    -- ========================================================
    -- PRODUCT
    -- ========================================================

    LEFT JOIN wsdbrksecommerce.gold.dim_product p

        ON s.product_id =
           p.product_id


    -- ========================================================
    -- SELLER
    -- ========================================================

    LEFT JOIN wsdbrksecommerce.gold.dim_seller se

        ON s.seller_id =
           se.seller_id


    -- ========================================================
    -- ORDER
    -- ========================================================

    LEFT JOIN wsdbrksecommerce.gold.dim_order o

        ON s.order_id =
           o.order_id


    -- ========================================================
    -- CATEGORY
    -- ========================================================

    LEFT JOIN wsdbrksecommerce.gold.dim_product_category pc

        ON s.product_category_name =
           pc.product_category_name


    -- ========================================================
    -- CUSTOMER STATISTICS
    -- ========================================================

    LEFT JOIN customer_stats cs

        ON s.customer_unique_id =
           cs.customer_unique_id
)


SELECT

    -- ========================================================
    -- KEYS
    -- ========================================================

    ROW_NUMBER() OVER (
        ORDER BY order_id, order_item_id
    ) AS order_item_key,

    order_key,

    product_key,

    seller_key,

    customer_key,

    product_category_key,


    -- ========================================================
    -- ORDER
    -- ========================================================

    order_id,

    order_item_id,

    order_status,

    order_business_status,


    -- ========================================================
    -- CUSTOMER
    -- ========================================================

    customer_id,

    customer_unique_id,

    customer_zip_code_prefix,

    customer_city,

    customer_state,

    customer_order_count,

    first_order_date,

    last_order_date,

    customer_segment,

    first_order_flag,


    -- ========================================================
    -- PRODUCT
    -- ========================================================

    product_id,

    product_category_name,

    product_category_name_english,

    product_weight_g,

    product_length_cm,

    product_height_cm,

    product_width_cm,


    -- ========================================================
    -- SELLER
    -- ========================================================

    seller_id,

    seller_zip_code_prefix,

    seller_city,

    seller_state,


    -- ========================================================
    -- DATE
    -- ========================================================

    purchase_date,

    purchase_year,

    purchase_quarter,

    purchase_month,

    day_of_week,

    day_name,

    week_of_year,

    year_month,

    month_name,


    -- ========================================================
    -- TIMESTAMPS
    -- ========================================================

    order_purchase_timestamp,

    order_approved_at,

    order_delivered_carrier_date,

    order_delivered_customer_date,

    order_estimated_delivery_date,


    -- ========================================================
    -- SALES
    -- ========================================================

    price,

    freight_value,

    quantity,

    total_item_value,


    -- ========================================================
    -- DELIVERY
    -- ========================================================

    delivery_days,

    delivery_delay_days,

    delivery_status,


    -- ========================================================
    -- STATUS FLAGS
    -- ========================================================

    delivered_flag,

    cancelled_flag,

    unavailable_flag,


    -- ========================================================
    -- DELIVERY FLAGS
    -- ========================================================

    late_delivery_flag,

    on_time_delivery_flag,


    -- ========================================================
    -- DATA QUALITY FLAGS
    -- ========================================================

    invalid_price_flag,

    invalid_freight_flag,

    invalid_quantity_flag,

    missing_customer_flag,

    missing_product_flag,

    missing_seller_flag,

    missing_category_flag,


    -- ========================================================
    -- AUDIT
    -- ========================================================

    CURRENT_TIMESTAMP()
        AS gold_created_timestamp


FROM fact_data

{% endset %}

{% do run_query(sql) %}


-- ============================================================
-- COMPLETED
-- ============================================================

{% do log(
    'Gold layer completed successfully - 6 tables created.',
    info=True
) %}

{% endmacro %}