from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
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


def save_to_postgres(df, table_name):
    df.write.format("jdbc").option("url", DB_URL).option("dbtable", table_name).option(
        "user", POSTGRES_USER
    ).option("password", POSTGRES_PASSWORD).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()


df_src = (
    spark.read.format("jdbc")
    .options(
        url=DB_URL,
        dbtable="mock",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        driver="org.postgresql.Driver",
    )
    .load()
)


def extract_dim_customers(df_src=df_src):
    # Выбираем нужные поля
    df_customers = df_src.select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code"),
        col("customer_pet_type").alias("pet_type"),
        col("customer_pet_name").alias("pet_name"),
        col("customer_pet_breed").alias("pet_breed"),
    )

    # Удаляем дубликаты по email, оставляя одну строку на каждого клиента
    window = Window.partitionBy("email").orderBy("email")
    df_customers = df_customers.withColumn("row_num", row_number().over(window))
    df_customers = df_customers.filter(col("row_num") == 1).drop("row_num")

    return df_customers


def extract_dim_sellers(df_src=df_src):
    df_sellers = df_src.select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code"),
    )

    window = Window.partitionBy("email").orderBy("email")
    df_sellers = df_sellers.withColumn("row_num", row_number().over(window))
    df_sellers = df_sellers.filter(col("row_num") == 1).drop("row_num")

    return df_sellers


def extract_dim_products(df_src=df_src):
    df_products = df_src.select(
        col("product_name").alias("name"),
        col("product_category").alias("category"),
        col("product_price").alias("price"),
        col("product_weight").alias("weight"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_brand").alias("brand"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("product_release_date").alias("release_date"),
        col("product_expiry_date").alias("expiry_date"),
        col("pet_category").alias("pet_category"),
    )

    window = Window.partitionBy("name", "category").orderBy("name")
    df_products = df_products.withColumn("row_num", row_number().over(window))
    df_products = df_products.filter(col("row_num") == 1).drop("row_num")

    return df_products


def extract_dim_stores(df_src=df_src):
    df_stores = df_src.select(
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email"),
    )

    window = Window.partitionBy("email").orderBy("email")
    df_stores = df_stores.withColumn("row_num", row_number().over(window))
    df_stores = df_stores.filter(col("row_num") == 1).drop("row_num")

    return df_stores


def extract_dim_suppliers(df_src=df_src):
    df_suppliers = df_src.select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country"),
    )

    window = Window.partitionBy("email").orderBy("email")
    df_suppliers = df_suppliers.withColumn("row_num", row_number().over(window))
    df_suppliers = df_suppliers.filter(col("row_num") == 1).drop("row_num")

    return df_suppliers


def load_dimension(table_name):
    return (
        spark.read.format("jdbc")
        .options(
            url=DB_URL,
            dbtable=table_name,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            driver="org.postgresql.Driver",
        )
        .load()
    )


def build_fact_sales(df_src=df_src):
    df_dim_customers = load_dimension("dim_customers")
    df_dim_sellers = load_dimension("dim_sellers")
    df_dim_products = load_dimension("dim_products")
    df_dim_stores = load_dimension("dim_stores")
    df_dim_suppliers = load_dimension("dim_suppliers")

    df_fact = (
        df_src.join(
            df_dim_customers, df_src.customer_email == df_dim_customers.email, "inner"
        )
        .join(df_dim_sellers, df_src.seller_email == df_dim_sellers.email, "inner")
        .join(
            df_dim_products,
            (df_src.product_name == df_dim_products.name)
            & (df_src.product_category == df_dim_products.category),
            "inner",
        )
        .join(df_dim_stores, df_src.store_email == df_dim_stores.email, "inner")
        .join(
            df_dim_suppliers, df_src.supplier_email == df_dim_suppliers.email, "inner"
        )
        .select(
            col("sale_date"),
            col("sale_quantity").alias("quantity"),
            col("sale_total_price").alias("total_price"),
            df_dim_customers.customer_id,
            df_dim_sellers.seller_id,
            df_dim_products.product_id,
            df_dim_stores.store_id,
            df_dim_suppliers.supplier_id,
        )
    )

    return df_fact


if __name__ == "__main__":
    df_dim_customers = extract_dim_customers()
    save_to_postgres(df_dim_customers, "dim_customers")

    df_dim_sellers = extract_dim_sellers()
    save_to_postgres(df_dim_sellers, "dim_sellers")

    df_dim_products = extract_dim_products()
    save_to_postgres(df_dim_products, "dim_products")

    df_dim_stores = extract_dim_stores()
    save_to_postgres(df_dim_stores, "dim_stores")

    df_dim_suppliers = extract_dim_suppliers()
    save_to_postgres(df_dim_suppliers, "dim_suppliers")

    df_fact_sales = build_fact_sales()
    save_to_postgres(df_fact_sales, "fact_sales")
