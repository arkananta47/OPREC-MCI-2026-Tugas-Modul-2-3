from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import glob
import os

def run_spark_analytics():

    # =========================================================
    # SPARK SESSION
    # =========================================================

    spark = SparkSession.builder \
        .appName("Orders_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca data parquet...")

    df_raw = spark.read.parquet(
        "file:///opt/airflow/data_lake/orders/"
    )

    # =========================================================
    # CAST DATA TYPE
    # =========================================================

    df_raw = df_raw \
        .withColumn(
            "quantity",
            F.col("quantity").cast("int")
        ) \
        .withColumn(
            "unit_price",
            F.col("unit_price").cast("double")
        ) \
        .withColumn(
            "total_price",
            F.col("total_price").cast("double")
        ) \
        .withColumn(
            "discount",
            F.col("discount").cast("double")
        ) \
        .withColumn(
            "tax",
            F.col("tax").cast("double")
        ) \
        .withColumn(
            "reordered",
            F.col("reordered").cast("int")
        )

    print("Menghitung analytics...")

    # =========================================================
    # TOP PRODUCTS
    # =========================================================

    trending_products = df_raw.groupBy(
        "product_id",
        "product_name",
        "category"
    ).agg(
        F.countDistinct("order_id").alias(
            "total_orders"
        ),

        F.sum("quantity").alias(
            "total_qty_sold"
        ),

        F.sum("reordered").alias(
            "total_reordered"
        ),

        F.sum("total_price").alias(
            "total_revenue"
        )

    ).orderBy(
        F.desc("total_qty_sold")
    ).limit(30)

    # =========================================================
    # CATEGORY SUMMARY
    # =========================================================

    category_summary = df_raw.groupBy(
        "category"
    ).agg(

        F.countDistinct("order_id").alias(
            "total_orders"
        ),

        F.sum("quantity").alias(
            "total_items_sold"
        ),

        F.avg("reordered").alias(
            "reorder_rate"
        ),

        F.sum("total_price").alias(
            "total_revenue"
        )

    ).orderBy(
        F.desc("total_items_sold")
    )

    # =========================================================
    # DAILY ORDERS
    # =========================================================

    daily_orders = df_raw.groupBy(
        "order_date"
    ).agg(

        F.countDistinct("order_id").alias(
            "total_orders"
        ),

        F.sum("quantity").alias(
            "total_items"
        ),

        F.sum("total_price").alias(
            "daily_revenue"
        )

    ).orderBy("order_date")

    # =========================================================
    # TO PANDAS
    # =========================================================

    df_products = trending_products.toPandas()
    df_categories = category_summary.toPandas()
    df_daily = daily_orders.toPandas()
    df_all = df_raw.toPandas()

    # =========================================================
    # FIX NUMPY TYPE
    # =========================================================

    df_products = df_products.astype({
        "total_orders": "int32",
        "total_qty_sold": "int32",
        "total_reordered": "int32"
    })

    df_categories = df_categories.astype({
        "total_orders": "int32",
        "total_items_sold": "int32"
    })

    df_daily = df_daily.astype({
        "total_orders": "int32",
        "total_items": "int32"
    })

    df_all = df_all.astype({
        "quantity": "int32",
        "reordered": "int32"
    })

    # =========================================================
    # CLICKHOUSE
    # =========================================================

    print("Memuat ke ClickHouse...")

    client = Client(
        host='clickhouse-server',
        user='kelompok28',
        password='kelompok28'
    )

    client.execute(
        'CREATE DATABASE IF NOT EXISTS mci2026_db'
    )

    # =========================================================
    # TABLE: orders_trending_products
    # =========================================================

    client.execute(
        'DROP TABLE IF EXISTS mci2026_db.orders_trending_products'
    )

    client.execute('''
        CREATE TABLE mci2026_db.orders_trending_products (

            product_id String,
            product_name String,
            category String,

            total_orders Int32,
            total_qty_sold Int32,
            total_reordered Int32,
            total_revenue Float64

        )
        ENGINE = MergeTree()
        ORDER BY total_qty_sold
    ''')

    data_products = [
        tuple(x)
        for x in df_products.to_numpy()
    ]

    if data_products:
        client.execute(
            'INSERT INTO mci2026_db.orders_trending_products VALUES',
            data_products
        )

    print(
        f"✅ orders_trending_products: "
        f"{len(data_products)} rows"
    )

    # =========================================================
    # TABLE: orders_category_summary
    # =========================================================

    client.execute(
        'DROP TABLE IF EXISTS mci2026_db.orders_category_summary'
    )

    client.execute('''
        CREATE TABLE mci2026_db.orders_category_summary (

            category String,

            total_orders Int32,
            total_items_sold Int32,

            reorder_rate Float64,
            total_revenue Float64

        )
        ENGINE = MergeTree()
        ORDER BY total_items_sold
    ''')

    data_categories = [
        tuple(x)
        for x in df_categories.to_numpy()
    ]

    if data_categories:
        client.execute(
            'INSERT INTO mci2026_db.orders_category_summary VALUES',
            data_categories
        )

    print(
        f"✅ orders_category_summary: "
        f"{len(data_categories)} rows"
    )

    # =========================================================
    # TABLE: orders_daily_orders
    # =========================================================

    client.execute(
        'DROP TABLE IF EXISTS mci2026_db.orders_daily_orders'
    )

    client.execute('''
        CREATE TABLE mci2026_db.orders_daily_orders (

            order_date String,

            total_orders Int32,
            total_items Int32,
            daily_revenue Float64

        )
        ENGINE = MergeTree()
        ORDER BY order_date
    ''')

    data_daily = [
        tuple(x)
        for x in df_daily.to_numpy()
    ]

    if data_daily:
        client.execute(
            'INSERT INTO mci2026_db.orders_daily_orders VALUES',
            data_daily
        )

    print(
        f"✅ orders_daily_orders: "
        f"{len(data_daily)} rows"
    )

    # =========================================================
    # TABLE: orders
    # =========================================================

    client.execute(
        'DROP TABLE IF EXISTS mci2026_db.orders'
    )

    client.execute('''
        CREATE TABLE mci2026_db.orders (

            order_id String,
            customer_id String,

            product_id String,
            product_name String,
            category String,

            quantity Int32,

            unit_price Float64,
            total_price Float64,

            discount Float64,
            tax Float64,

            status String,

            payment_method String,

            shipping_city String,
            shipping_country String,

            reordered Int32,

            order_date String,

            updated_at String,
            ingested_at String

        )
        ENGINE = MergeTree()

        PARTITION BY substring(order_date, 1, 7)

        ORDER BY (order_date, order_id)
    ''')

    data_all = [
        tuple(x)
        for x in df_all.to_numpy()
    ]

    if data_all:
        client.execute(
            'INSERT INTO mci2026_db.orders VALUES',
            data_all
        )

    print(f"✅ orders: {len(data_all)} rows")

    # =========================================================
    # CLEANUP
    # =========================================================

    print("Membersihkan parquet lama...")

    files = glob.glob(
        '/opt/airflow/data_lake/orders/*.parquet'
    )

    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error deleting {f}: {e}")

    spark.stop()

    print("✅ Pipeline selesai")


if __name__ == "__main__":
    run_spark_analytics()