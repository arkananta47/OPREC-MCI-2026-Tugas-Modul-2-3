-- MCI2026 Task 2 — Metabase Queries 
-- Dataset: Grocery Orders
-- Database: mci2026_db


-- Q1: Top 30 Most Ordered Products
-- Produk paling sering dibeli
SELECT
    product_name,
    category,
    total_orders,
    total_qty_sold,
    total_reordered,
    total_revenue
FROM mci2026_db.orders_trending_products
ORDER BY total_qty_sold DESC
LIMIT 30;


-- Q2: Most Popular Categories
-- Kategori dengan item paling banyak terjual
SELECT
    category,
    total_orders,
    total_items_sold,
    round(reorder_rate * 100, 2) AS reorder_rate_percent,
    total_revenue
FROM mci2026_db.orders_category_summary
ORDER BY total_items_sold DESC;


-- Q3: Reorder Distribution
-- Persentase reorder vs first purchase
SELECT
    CASE
        WHEN reordered = 1 THEN 'Reordered'
        ELSE 'First Purchase'
    END AS reorder_status,
    count() AS total_items,
    round(
        count() * 100.0 /
        sum(count()) OVER (),
        2
    ) AS percentage
FROM mci2026_db.orders
GROUP BY reorder_status
ORDER BY total_items DESC;


-- Q4: Payment Method Distribution
-- Distribusi metode pembayaran
SELECT
    payment_method,
    count() AS total_transactions,
    round(
        sum(total_price),
        2
    ) AS total_revenue
FROM mci2026_db.orders
GROUP BY payment_method
ORDER BY total_transactions DESC;


-- Q5: Top Shipping Cities
-- Kota dengan transaksi terbanyak
SELECT
    shipping_city,
    countDistinct(order_id) AS total_orders,
    round(
        sum(total_price),
        2
    ) AS total_revenue
FROM mci2026_db.orders
GROUP BY shipping_city
ORDER BY total_orders DESC;


-- Q6: Basket Size Distribution
-- Distribusi jumlah item per order
SELECT
    CASE
        WHEN item_count <= 5 THEN '1-5 Items'
        WHEN item_count <= 10 THEN '6-10 Items'
        WHEN item_count <= 20 THEN '11-20 Items'
        ELSE '20+ Items'
    END AS basket_size,
    count() AS total_orders
FROM (
    SELECT
        order_id,
        count() AS item_count
    FROM mci2026_db.orders
    GROUP BY order_id
)
GROUP BY basket_size
ORDER BY total_orders DESC;


-- Q7: Top Reordered Products
-- Produk dengan reorder rate tertinggi
SELECT
    product_name,
    category,
    sum(reordered) AS total_reordered,
    count() AS total_orders,
    round(
        sum(reordered) * 100.0 /
        count(),
        2
    ) AS reorder_rate_pct
FROM mci2026_db.orders
GROUP BY product_name, category
HAVING total_orders >= 5
ORDER BY reorder_rate_pct DESC
LIMIT 20;


-- Q8: KPI Summary Dashboard
-- Ringkasan utama dataset
SELECT
    countDistinct(order_id) AS total_orders,
    countDistinct(customer_id) AS unique_customers,
    countDistinct(product_id) AS unique_products,
    countDistinct(category) AS unique_categories,
    count() AS total_items_sold,
    round(
        sum(total_price),
        2
    ) AS total_revenue,
    round(
        avg(total_price),
        2
    ) AS avg_transaction_value,
    round(
        avg(reordered) * 100,
        2
    ) AS overall_reorder_rate_pct
FROM mci2026_db.orders;


-- Q9: Revenue by Category
-- Total revenue tiap kategori
SELECT
    category,
    count(product_id) AS total_products_sold,
    round(
        sum(total_price),
        2
    ) AS total_revenue
FROM mci2026_db.orders
GROUP BY category
ORDER BY total_revenue DESC;