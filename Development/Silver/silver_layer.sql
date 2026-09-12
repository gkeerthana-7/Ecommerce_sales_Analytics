-- ============================================================
-- macros/silver_layer.sql
-- E-Commerce Sales Analytics - Silver Layer
-- ============================================================

{% macro silver_layer() %}


-- ============================================================
-- silver_customer_dataset
-- ============================================================

{% set customers_query %}

CREATE OR REPLACE TABLE wsdbrksecommerce.silver.silver_customer_dataset AS

WITH customers AS (

    SELECT
        CAST(customer_id AS STRING) AS customer_id,
        CAST(customer_unique_id AS STRING) AS customer_unique_id,

        TRY_CAST(customer_zip_code_prefix AS INT)
            AS customer_zip_code_prefix,

        TRIM(LOWER(customer_city)) AS customer_city,

        UPPER(TRIM(customer_state)) AS customer_state

    FROM {{ source('bronze', 'customer_dataset') }}
),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY customer_unique_id
        ) AS rn

    FROM customers
)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,

    COALESCE(customer_city, 'unknown') AS customer_city,

    COALESCE(customer_state, 'UNKNOWN') AS customer_state,

    CURRENT_TIMESTAMP() AS ingestion_timestamp

FROM deduplicated

WHERE rn = 1
  AND customer_id IS NOT NULL
  AND customer_unique_id IS NOT NULL
  AND customer_zip_code_prefix IS NOT NULL
  AND customer_zip_code_prefix >= 0

{% endset %}

{{ log('START: silver_customer_dataset', info=True) }}
{% do run_query(customers_query) %}


-- ============================================================
-- silver_orders_dataset
-- ============================================================

{% set orders_query %}

CREATE OR REPLACE TABLE wsdbrksecommerce.silver.silver_orders_dataset AS

WITH orders AS (

    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(customer_id AS STRING) AS customer_id,

        LOWER(TRIM(order_status)) AS order_status,

        TRY_CAST(
            order_purchase_timestamp AS TIMESTAMP
        ) AS order_purchase_timestamp,

        TRY_CAST(
            order_approved_at AS TIMESTAMP
        ) AS order_approved_at,

        TRY_CAST(
            order_delivered_carrier_date AS TIMESTAMP
        ) AS order_delivered_carrier_date,

        TRY_CAST(
            order_delivered_customer_date AS TIMESTAMP
        ) AS order_delivered_customer_date,

        TRY_CAST(
            order_estimated_delivery_date AS TIMESTAMP
        ) AS order_estimated_delivery_date

    FROM {{ source('bronze', 'orders_dataset') }}
),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_purchase_timestamp
        ) AS rn

    FROM orders
)

SELECT

    order_id,
    customer_id,

    COALESCE(order_status, 'unknown') AS order_status,

    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    CAST(
        order_purchase_timestamp AS DATE
    ) AS purchase_date,

    YEAR(order_purchase_timestamp)
        AS purchase_year,

    QUARTER(order_purchase_timestamp)
        AS purchase_quarter,

    MONTH(order_purchase_timestamp)
        AS purchase_month,

    CASE

        WHEN order_delivered_customer_date IS NOT NULL
         AND order_purchase_timestamp IS NOT NULL

        THEN DATEDIFF(
            CAST(order_delivered_customer_date AS DATE),
            CAST(order_purchase_timestamp AS DATE)
        )

        ELSE NULL

    END AS delivery_days,

    CASE

        WHEN order_delivered_customer_date IS NOT NULL
         AND order_estimated_delivery_date IS NOT NULL

        THEN DATEDIFF(
            CAST(order_delivered_customer_date AS DATE),
            CAST(order_estimated_delivery_date AS DATE)
        )

        ELSE NULL

    END AS delivery_delay_days,

    CASE

        WHEN order_status = 'delivered'
         AND order_delivered_customer_date IS NOT NULL
         AND order_estimated_delivery_date IS NOT NULL
         AND order_delivered_customer_date >
             order_estimated_delivery_date

        THEN 'Late'

        WHEN order_status = 'delivered'
         AND order_delivered_customer_date IS NOT NULL
         AND order_estimated_delivery_date IS NOT NULL
         AND order_delivered_customer_date <=
             order_estimated_delivery_date

        THEN 'On-Time'

        ELSE 'Unknown'

    END AS delivery_status,

    CASE

        WHEN order_status = 'canceled'
            THEN 'Cancelled'

        WHEN order_status = 'unavailable'
            THEN 'Unavailable'

        WHEN order_status = 'delivered'
            THEN 'Delivered'

        ELSE 'Active'

    END AS order_business_status,

    CURRENT_TIMESTAMP() AS ingestion_timestamp

FROM deduplicated

WHERE rn = 1
  AND order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_purchase_timestamp IS NOT NULL

  AND (
        order_approved_at IS NULL
        OR order_approved_at >= order_purchase_timestamp
      )

  AND (
        order_delivered_carrier_date IS NULL
        OR order_delivered_carrier_date >= order_purchase_timestamp
      )

  AND (
        order_delivered_customer_date IS NULL
        OR order_delivered_customer_date >= order_purchase_timestamp
      )

  AND (
        order_delivered_customer_date IS NULL
        OR order_delivered_carrier_date IS NULL
        OR order_delivered_customer_date >=
           order_delivered_carrier_date
      )

{% endset %}

{{ log('START: silver_orders_dataset', info=True) }}
{% do run_query(orders_query) %}


-- ============================================================
-- silver_order_items_dataset
-- ============================================================

{% set order_items_query %}

CREATE OR REPLACE TABLE wsdbrksecommerce.silver.silver_order_items_dataset AS

WITH order_items AS (

    SELECT

        CAST(order_id AS STRING)
            AS order_id,

        TRY_CAST(order_item_id AS INT)
            AS order_item_id,

        CAST(product_id AS STRING)
            AS product_id,

        CAST(seller_id AS STRING)
            AS seller_id,

        TRY_CAST(
            shipping_limit_date AS TIMESTAMP
        ) AS shipping_limit_date,

        TRY_CAST(
            price AS DECIMAL(18,2)
        ) AS price,

        TRY_CAST(
            freight_value AS DECIMAL(18,2)
        ) AS freight_value

    FROM {{ source('bronze', 'order_items_dataset') }}
),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id
            ORDER BY shipping_limit_date
        ) AS rn

    FROM order_items
)

SELECT

    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,

    price,
    freight_value,

    CAST(
        price + freight_value
        AS DECIMAL(18,2)
    ) AS total_item_value,

    1 AS item_quantity,

    CURRENT_TIMESTAMP() AS ingestion_timestamp

FROM deduplicated

WHERE rn = 1
  AND order_id IS NOT NULL
  AND order_item_id IS NOT NULL
  AND product_id IS NOT NULL
  AND seller_id IS NOT NULL

  AND price IS NOT NULL
  AND freight_value IS NOT NULL

  AND price >= 0
  AND freight_value >= 0

{% endset %}

{{ log('START: silver_order_items_dataset', info=True) }}
{% do run_query(order_items_query) %}


-- ============================================================
-- silver_products_dataset
-- ============================================================

{% set products_query %}

CREATE OR REPLACE TABLE wsdbrksecommerce.silver.silver_products_dataset AS

WITH products AS (

    SELECT

        CAST(product_id AS STRING)
            AS product_id,

        TRIM(LOWER(product_category_name))
            AS product_category_name,

        -- STRING → DOUBLE → INT
        TRY_CAST(
            TRY_CAST(product_name_lenght AS DOUBLE) AS INT
        ) AS product_name_length,

        TRY_CAST(
            TRY_CAST(product_description_lenght AS DOUBLE) AS INT
        ) AS product_description_length,

        TRY_CAST(
            TRY_CAST(product_photos_qty AS DOUBLE) AS INT
        ) AS product_photos_qty,

        -- Product dimensions
        TRY_CAST(
            product_weight_g AS DECIMAL(18,2)
        ) AS product_weight_g,

        TRY_CAST(
            product_length_cm AS DECIMAL(18,2)
        ) AS product_length_cm,

        TRY_CAST(
            product_height_cm AS DECIMAL(18,2)
        ) AS product_height_cm,

        TRY_CAST(
            product_width_cm AS DECIMAL(18,2)
        ) AS product_width_cm

    FROM {{ source('bronze', 'products_dataset') }}
),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_id
        ) AS rn

    FROM products
),

medians AS (

    SELECT

        percentile_approx(
            product_name_length,
            0.5
        ) AS median_name_length,

        percentile_approx(
            product_description_length,
            0.5
        ) AS median_description_length,

        percentile_approx(
            product_photos_qty,
            0.5
        ) AS median_photos_qty,

        percentile_approx(
            product_weight_g,
            0.5
        ) AS median_weight,

        percentile_approx(
            product_length_cm,
            0.5
        ) AS median_length,

        percentile_approx(
            product_height_cm,
            0.5
        ) AS median_height,

        percentile_approx(
            product_width_cm,
            0.5
        ) AS median_width

    FROM deduplicated

    WHERE rn = 1
)

SELECT

    d.product_id,

    COALESCE(
        d.product_category_name,
        'unknown'
    ) AS product_category_name,


    -- ========================================================
    -- PRODUCT NAME / DESCRIPTION / PHOTOS
    -- ========================================================

    COALESCE(
        d.product_name_length,
        m.median_name_length,
        0
    ) AS product_name_length,

    COALESCE(
        d.product_description_length,
        m.median_description_length,
        0
    ) AS product_description_length,

    COALESCE(
        d.product_photos_qty,
        m.median_photos_qty,
        0
    ) AS product_photos_qty,


    -- ========================================================
    -- PRODUCT DIMENSIONS
    -- ========================================================

    COALESCE(
        d.product_weight_g,
        m.median_weight,
        0
    ) AS product_weight_g,

    COALESCE(
        d.product_length_cm,
        m.median_length,
        0
    ) AS product_length_cm,

    COALESCE(
        d.product_height_cm,
        m.median_height,
        0
    ) AS product_height_cm,

    COALESCE(
        d.product_width_cm,
        m.median_width,
        0
    ) AS product_width_cm,


    CURRENT_TIMESTAMP() AS ingestion_timestamp

FROM deduplicated d

CROSS JOIN medians m

WHERE d.rn = 1
  AND d.product_id IS NOT NULL

  AND (
        d.product_name_length IS NULL
        OR d.product_name_length >= 0
      )

  AND (
        d.product_description_length IS NULL
        OR d.product_description_length >= 0
      )

  AND (
        d.product_photos_qty IS NULL
        OR d.product_photos_qty >= 0
      )

  AND (
        d.product_weight_g IS NULL
        OR d.product_weight_g >= 0
      )

  AND (
        d.product_length_cm IS NULL
        OR d.product_length_cm >= 0
      )

  AND (
        d.product_height_cm IS NULL
        OR d.product_height_cm >= 0
      )

  AND (
        d.product_width_cm IS NULL
        OR d.product_width_cm >= 0
      )

{% endset %}

{{ log('START: silver_products_dataset', info=True) }}
{% do run_query(products_query) %}


-- ============================================================
-- silver_product_category_name_translation
-- ============================================================

{% set category_query %}

CREATE OR REPLACE TABLE
wsdbrksecommerce.silver.silver_product_category_name_translation AS

WITH categories AS (

    SELECT

        TRIM(LOWER(product_category_name))
            AS product_category_name,

        TRIM(LOWER(product_category_name_english))
            AS product_category_name_english

    FROM {{ source(
        'bronze',
        'product_category_name_translation'
    ) }}
),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_category_name
            ORDER BY product_category_name_english
        ) AS rn

    FROM categories
)

SELECT

    product_category_name,

    COALESCE(
        product_category_name_english,
        'unknown'
    ) AS product_category_name_english,

    CURRENT_TIMESTAMP() AS ingestion_timestamp

FROM deduplicated

WHERE rn = 1
  AND product_category_name IS NOT NULL

{% endset %}

{{ log('START: silver_product_category_name_translation', info=True) }}
{% do run_query(category_query) %}


-- ============================================================
-- silver_sellers_dataset
-- ============================================================

{% set sellers_query %}

CREATE OR REPLACE TABLE
wsdbrksecommerce.silver.silver_sellers_dataset AS

WITH sellers AS (

    SELECT

        CAST(seller_id AS STRING)
            AS seller_id,

        TRY_CAST(
            seller_zip_code_prefix AS INT
        ) AS seller_zip_code_prefix,

        TRIM(LOWER(seller_city))
            AS seller_city,

        UPPER(TRIM(seller_state))
            AS seller_state

    FROM {{ source('bronze', 'sellers_dataset') }}
),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY seller_id
        ) AS rn

    FROM sellers
)

SELECT

    seller_id,

    seller_zip_code_prefix,

    COALESCE(
        seller_city,
        'unknown'
    ) AS seller_city,

    COALESCE(
        seller_state,
        'UNKNOWN'
    ) AS seller_state,

    CURRENT_TIMESTAMP() AS ingestion_timestamp

FROM deduplicated

WHERE rn = 1
  AND seller_id IS NOT NULL
  AND seller_zip_code_prefix IS NOT NULL
  AND seller_zip_code_prefix >= 0

{% endset %}

{{ log('START: silver_sellers_dataset', info=True) }}
{% do run_query(sellers_query) %}


-- ============================================================
-- silver_sales
-- ============================================================

{% set sales_query %}

CREATE OR REPLACE TABLE wsdbrksecommerce.silver.silver_sales AS

SELECT

    -- Order information
    oi.order_id,
    oi.order_item_id,

    -- Customer information
    -- customer_id comes from orders
    o.customer_id,
    c.customer_unique_id,

    -- Product and seller information
    oi.product_id,
    oi.seller_id,

    -- Order status
    o.order_status,
    o.order_business_status,

    -- Order timestamps
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Date attributes
    o.purchase_date,
    o.purchase_year,
    o.purchase_quarter,
    o.purchase_month,

    -- Delivery metrics
    o.delivery_days,
    o.delivery_delay_days,
    o.delivery_status,

    -- Order item metrics
    oi.shipping_limit_date,
    oi.price,
    oi.freight_value,
    oi.total_item_value,
    oi.item_quantity,

    -- Product information
    p.product_category_name,

    COALESCE(
        ct.product_category_name_english,
        'unknown'
    ) AS product_category_name_english,

    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,

    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    -- Customer geography
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,

    -- Seller geography
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,

    -- Audit column
    CURRENT_TIMESTAMP() AS ingestion_timestamp

FROM wsdbrksecommerce.silver.silver_order_items_dataset oi

-- Order item → Order
INNER JOIN wsdbrksecommerce.silver.silver_orders_dataset o
    ON oi.order_id = o.order_id

-- Order → Customer
INNER JOIN wsdbrksecommerce.silver.silver_customer_dataset c
    ON o.customer_id = c.customer_id

-- Order item → Product
LEFT JOIN wsdbrksecommerce.silver.silver_products_dataset p
    ON oi.product_id = p.product_id

-- Order item → Seller
LEFT JOIN wsdbrksecommerce.silver.silver_sellers_dataset s
    ON oi.seller_id = s.seller_id

-- Product → Category Translation
LEFT JOIN wsdbrksecommerce.silver.silver_product_category_name_translation ct
    ON p.product_category_name =
       ct.product_category_name

{% endset %}

{{ log('START: silver_sales', info=True) }}
{% do run_query(sales_query) %}

{% endmacro %}