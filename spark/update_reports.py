import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    desc,
    first,
    lpad,
    month,
    row_number,
    sum,
    year,
)
from pyspark.sql.window import Window

DB_URL = "jdbc:postgresql://postgres:5432/spark_db"
POSTGRES_USER = "spark_user"
POSTGRES_PASSWORD = "spark_password"

spark = (
    SparkSession.builder.appName("PostgreSQL-Integration")
    .config("spark.jars", "/home/jovyan/postgresql-jdbc.jar")
    .config("spark.driver.extraClassPath", "/home/jovyan/postgresql-jdbc.jar")
    .config("spark.executor.extraClassPath", "/home/jovyan/postgresql-jdbc.jar")
    .getOrCreate()
)

# === 1. Чтение таблиц из PostgreSQL ===
dim_customers = (
    spark.read.format("jdbc")
    .option("url", DB_URL)
    .option("dbtable", "dim_customers")
    .options(user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    .load()
)
dim_sellers = (
    spark.read.format("jdbc")
    .option("url", DB_URL)
    .option("dbtable", "dim_sellers")
    .options(user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    .load()
)
dim_products = (
    spark.read.format("jdbc")
    .option("url", DB_URL)
    .option("dbtable", "dim_products")
    .options(user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    .load()
)
dim_stores = (
    spark.read.format("jdbc")
    .option("url", DB_URL)
    .option("dbtable", "dim_stores")
    .options(user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    .load()
)
dim_suppliers = (
    spark.read.format("jdbc")
    .option("url", DB_URL)
    .option("dbtable", "dim_suppliers")
    .options(user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    .load()
)
fact_sales = (
    spark.read.format("jdbc")
    .option("url", DB_URL)
    .option("dbtable", "fact_sales")
    .options(user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    .load()
)

# =======================================
# === Витрина 1: Продажи по продуктам ===
# =======================================

# === 1. Агрегация по продуктам ===
product_stats = (
    fact_sales.alias("fs")
    .join(dim_products.alias("p"), col("fs.product_id") == col("p.product_id"))
    .groupBy("fs.product_id", "p.name", "p.category", "p.rating", "p.reviews")
    .agg(
        sum("fs.quantity").alias("total_sold"),
        sum("fs.total_price").alias("total_revenue"),
    )
)

# === 2. Выбор топ-10 по количеству продаж ===
window = Window.orderBy(desc("total_sold"))
top_10_products = (
    product_stats.withColumn("row_number", row_number().over(window))
    .filter(col("row_number") <= 10)
    .drop("row_number")
)

# === 3. Агрегация общей выручки по категориям ===
category_revenue = (
    fact_sales.alias("fs")
    .join(dim_products.alias("p"), col("fs.product_id") == col("p.product_id"))
    .groupBy("p.category")
    .agg(sum("fs.total_price").alias("category_total_revenue"))
)

# === 4. Джойн для добавления общей выручки категории ===
report_products = (
    top_10_products.alias("t10")
    .join(category_revenue.alias("cat"), col("t10.category") == col("cat.category"))
    .select(
        col("t10.product_id"),
        col("t10.name").alias("product_name"),
        col("t10.category").alias("product_category"),
        col("t10.total_sold"),
        col("t10.total_revenue"),
        col("cat.category_total_revenue"),
        col("t10.rating").alias("avg_rating"),
        col("t10.reviews").alias("total_reviews"),
    )
    .orderBy(desc("total_sold"))
)

# ======================================
# === Витрина 2: Продажи по клиентам ===
# ======================================

# === 1. Агрегация по клиентам ===
customer_stats = (
    fact_sales.alias("fs")
    .join(dim_customers.alias("c"), col("fs.customer_id") == col("c.customer_id"))
    .groupBy("fs.customer_id", "c.email", "c.first_name", "c.last_name", "c.country")
    .agg(
        sum("fs.total_price").alias("total_spent"),
        avg("fs.total_price").alias("avg_check"),
    )
)

# === 2. Отбор топ-10 клиентов по общей сумме покупок ===
window = Window.orderBy(desc("total_spent"))
report_customers = (
    customer_stats.withColumn("row_number", row_number().over(window))
    .filter(col("row_number") <= 10)
    .drop("row_number")
    .orderBy(desc("total_spent"))
)

# =====================================
# === Витрина 3: Продажи по времени ===
# =====================================

report_time = (
    fact_sales.withColumn("sale_month", month("sale_date"))
    .withColumn("sale_year", year("sale_date"))
    .withColumn("month_padded", lpad(col("sale_month"), 2, "0"))
    .withColumn("year_month", concat_ws("-", col("sale_year"), col("month_padded")))
    .groupBy("sale_year", "sale_month", "year_month")
    .agg(
        sum("total_price").alias("monthly_revenue"),
        avg("total_price").alias("avg_order_value"),
        count("*").alias("total_orders"),
    )
    .orderBy("sale_year", "sale_month")
)

# =======================================
# === Витрина 4: Продажи по магазинам ===
# =======================================

# === 1. Агрегация по магазинам ===
store_stats = (
    fact_sales.join(dim_stores, "store_id")
    .groupBy("store_id", "name", "city", "country")
    .agg(
        sum("total_price").alias("store_revenue"),
        avg("total_price").alias("avg_check"),
    )
)

# === 2. Топ-5 магазинов по выручке ===
window = Window.orderBy(desc("store_revenue"))
top5_stores = (
    store_stats.withColumn("row_number", row_number().over(window))
    .filter(col("row_number") <= 5)
    .drop("row_number")
)

# === 3. Продажи по городам и странам ===
sales_by_city = (
    fact_sales.join(dim_stores, "store_id")
    .groupBy("city")
    .agg(count("*").alias("sales_in_city"))
)

sales_by_country = (
    fact_sales.join(dim_stores, "store_id")
    .groupBy("country")
    .agg(count("*").alias("sales_in_country"))
)

# === 4. Финальная витрина ===
report_stores = (
    top5_stores.join(sales_by_city, "city")
    .join(sales_by_country, "country")
    .select(
        col("store_id"),
        col("name").alias("store_name"),
        col("city"),
        col("country"),
        col("store_revenue"),
        col("avg_check"),
        col("sales_in_city"),
        col("sales_in_country"),
    )
    .orderBy(desc("store_revenue"))
)

# =========================================
# === Витрина 5: Продажи по поставщикам ===
# =========================================

# === 1. Агрегация по поставщикам ===
supplier_stats = (
    fact_sales.alias("fs")
    .join(dim_suppliers.alias("sup"), col("fs.supplier_id") == col("sup.supplier_id"))
    .join(dim_products.alias("p"), col("fs.product_id") == col("p.product_id"))
    .groupBy("fs.supplier_id", "sup.name", "sup.country")
    .agg(
        sum("fs.total_price").alias("supplier_revenue"),
        avg("p.price").alias("avg_price"),
    )
)

# === 2. Топ-5 поставщиков по выручке ===
window = Window.orderBy(desc("supplier_revenue"))
top5_suppliers = (
    supplier_stats.withColumn("row_number", row_number().over(window))
    .filter(col("row_number") <= 5)
    .drop("row_number")
)

# === 3. Продажи каждого поставщика в его стране ===
sales_per_supplier_country = (
    fact_sales.alias("fs")
    .join(dim_suppliers.alias("sup"), col("fs.supplier_id") == col("sup.supplier_id"))
    .groupBy("sup.supplier_id", "sup.country")
    .agg(count("*").alias("sales_in_country"))
)

# === 4. Финальная витрина ===
report_suppliers = (
    top5_suppliers.alias("top")
    .join(
        sales_per_supplier_country.alias("sc"),
        (col("top.supplier_id") == col("sc.supplier_id"))
        & (col("top.country") == col("sc.country")),
    )
    .select(
        col("top.supplier_id"),
        col("top.name").alias("supplier_name"),
        col("top.country"),
        col("top.supplier_revenue"),
        col("top.avg_price"),
        col("sc.sales_in_country"),
    )
    .orderBy(desc("supplier_revenue"))
)

# =====================================
# === Витрина 6: Качество продукции ===
# =====================================

# === 1. Базовая агрегация по продуктам ===
product_quality_stats = (
    fact_sales.alias("fs")
    .join(dim_products.alias("p"), col("fs.product_id") == col("p.product_id"))
    .groupBy("fs.product_id", "p.name", "p.category")
    .agg(
        first("p.rating").alias("avg_rating"),
        first("p.reviews").alias("total_reviews"),
        sum("fs.quantity").alias("total_sold"),
    )
    .withColumnRenamed("name", "product_name")
)

# === 2. Окна для флагов ===
window_rating_desc = Window.orderBy(desc("avg_rating"))
window_rating_asc = Window.orderBy(col("avg_rating"))
window_reviews = Window.orderBy(desc("total_reviews"))

# === 3. Добавляем флаги топов ===
report_quality = (
    product_quality_stats.withColumn("rank_top", row_number().over(window_rating_desc))
    .withColumn("rank_low", row_number().over(window_rating_asc))
    .withColumn("rank_reviews", row_number().over(window_reviews))
    .withColumn("is_top_rated", col("rank_top") <= 5)
    .withColumn("is_low_rated", col("rank_low") <= 5)
    .withColumn("is_most_reviewed", col("rank_reviews") <= 5)
    .drop("rank_top", "rank_low", "rank_reviews")
    .orderBy(desc("avg_rating"))
)

table_dataframes = {
    "report_products": report_products,
    "report_customers": report_customers,
    "report_time": report_time,
    "report_stores": report_stores,
    "report_suppliers": report_suppliers,
    "report_quality": report_quality,
}

for t_name, t in table_dataframes.items():
    print(t_name)
    print(t.toPandas())

# =====================================
# === Выгрузка отчетов в ClickHouse ===
# =====================================

# === 1. Подключение к ClickHouse ===
client = clickhouse_connect.get_client(
    host="clickhouse", port=8123, username="custom_user", password="custom_password"
)

# === 2. Определения таблиц ClickHouse ===
table_defs = {
    "report_products": """
DROP TABLE IF EXISTS report_products;
CREATE TABLE report_products (
    product_id UInt32,
    product_name String,
    product_category String,
    total_sold UInt32,
    total_revenue Float64,
    category_total_revenue Float64,
    avg_rating Float32,
    total_reviews UInt32
) ENGINE = MergeTree()
ORDER BY product_id;
    """,
    "report_customers": """
DROP TABLE IF EXISTS report_customers;
CREATE TABLE report_customers (
    customer_id UInt32,
    email String,
    first_name String,
    last_name String,
    country String,
    total_spent Float64,
    avg_check Float64
) ENGINE = MergeTree()
ORDER BY customer_id;
    """,
    "report_time": """
DROP TABLE IF EXISTS report_time;
CREATE TABLE report_time (
    sale_year UInt16,
    sale_month UInt8,
    year_month String,
    monthly_revenue Float64,
    avg_order_value Float64,
    total_orders UInt32
) ENGINE = MergeTree()
ORDER BY (sale_year, sale_month);
    """,
    "report_stores": """
DROP TABLE IF EXISTS report_stores;
CREATE TABLE report_stores (
    store_id UInt32,
    store_name String,
    city String,
    country String,
    store_revenue Float64,
    avg_check Float64,
    sales_in_city UInt32,
    sales_in_country UInt32
) ENGINE = MergeTree()
ORDER BY store_id;
    """,
    "report_suppliers": """
DROP TABLE IF EXISTS report_suppliers;
CREATE TABLE report_suppliers (
    supplier_id UInt32,
    supplier_name String,
    country String,
    supplier_revenue Float64,
    avg_price Float64,
    sales_in_country UInt32
) ENGINE = MergeTree()
ORDER BY supplier_id;
    """,
    "report_quality": """
DROP TABLE IF EXISTS report_quality;
CREATE TABLE report_quality (
    product_id UInt32,
    product_name String,
    category String,
    avg_rating Float32,
    total_reviews UInt32,
    total_sold UInt32,
    is_top_rated UInt8,
    is_low_rated UInt8,
    is_most_reviewed UInt8
) ENGINE = MergeTree()
ORDER BY product_id;
    """,
}

# === 3. Создание таблиц ===
for table_name, ddl in table_defs.items():
    # Разделяем на строки и выполняем по отдельности
    statements = ddl.strip().split(";")
    for statement in statements:
        stmt = statement.strip()
        if stmt:
            client.command(stmt)

# === 4. Запись данных в ClickHouse ===
for table_name, df in table_dataframes.items():
    pdf = df.toPandas()

    # Приводим к списку списков по колонкам
    data_columns = [pdf[col].tolist() for col in pdf.columns]

    client.insert(
        table_name,
        data=data_columns,
        column_names=list(pdf.columns),
        column_oriented=True,
    )

print("✅ Все 6 витрин успешно загружены в ClickHouse.")
