# update_data.py
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

DB_URL = "jdbc:postgresql://postgres:5432/bigdata_lab2"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"


def create_spark_session():
    return (
        SparkSession.builder.appName("PostgreSQL-Integration")
        .config("spark.jars", "/home/jovyan/postgresql-jdbc.jar")
        .config("spark.driver.extraClassPath", "/home/jovyan/postgresql-jdbc.jar")
        .config("spark.executor.extraClassPath", "/home/jovyan/postgresql-jdbc.jar")
        .getOrCreate()
    )


def process_data(spark):
    df_src = (
        spark.read.format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "mock")
        .options(user=POSTGRES_USER, password=POSTGRES_PASSWORD)
        .load()
    )

    # 1. Dim_Customer
    df_dim_customer = df_src.select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code"),
        col("customer_pet_type").alias("pet_type"),
        col("customer_pet_name").alias("pet_name"),
        col("customer_pet_breed").alias("pet_breed"),
    ).distinct()

    # Handle email conflicts (PySpark equivalent of ON CONFLICT DO NOTHING)
    windowSpec = Window.partitionBy("email").orderBy("email")
    df_dim_customer = df_dim_customer.withColumn(
        "row_num", row_number().over(windowSpec)
    )
    df_dim_customer = df_dim_customer.filter(col("row_num") == 1).drop("row_num")
    df_dim_customer = df_dim_customer.withColumn(
        "customer_id", monotonically_increasing_id()
    )

    # 2. Dim_Seller
    df_dim_seller = df_src.select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code"),
    ).distinct()

    # Handle email conflicts
    windowSpec = Window.partitionBy("email").orderBy("email")
    df_dim_seller = df_dim_seller.withColumn("row_num", row_number().over(windowSpec))
    df_dim_seller = df_dim_seller.filter(col("row_num") == 1).drop("row_num")
    df_dim_seller = df_dim_seller.withColumn("seller_id", monotonically_increasing_id())

    # 3. Dim_Product
    df_dim_product = df_src.select(
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
    ).distinct()

    # Handle (name, category) conflicts
    windowSpec = Window.partitionBy("name", "category").orderBy("name")
    df_dim_product = df_dim_product.withColumn("row_num", row_number().over(windowSpec))
    df_dim_product = df_dim_product.filter(col("row_num") == 1).drop("row_num")
    df_dim_product = df_dim_product.withColumn(
        "product_id", monotonically_increasing_id()
    )

    # 4. Dim_Store
    df_dim_store = df_src.select(
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email"),
    ).distinct()

    # Handle (name, location) conflicts
    windowSpec = Window.partitionBy("name", "location").orderBy("name")
    df_dim_store = df_dim_store.withColumn("row_num", row_number().over(windowSpec))
    df_dim_store = df_dim_store.filter(col("row_num") == 1).drop("row_num")
    df_dim_store = df_dim_store.withColumn("store_id", monotonically_increasing_id())

    # 5. Dim_Supplier
    df_dim_supplier = df_src.select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country"),
    ).distinct()

    # Handle email conflicts
    windowSpec = Window.partitionBy("email").orderBy("email")
    df_dim_supplier = df_dim_supplier.withColumn(
        "row_num", row_number().over(windowSpec)
    )
    df_dim_supplier = df_dim_supplier.filter(col("row_num") == 1).drop("row_num")
    df_dim_supplier = df_dim_supplier.withColumn(
        "supplier_id", monotonically_increasing_id()
    )

    # Fact Table Creation
    df_fact_sales = (
        df_src.join(
            df_dim_customer, df_src.customer_email == df_dim_customer.email, "inner"
        )
        .join(df_dim_seller, df_src.seller_email == df_dim_seller.email, "inner")
        .join(
            df_dim_product,
            (df_src.product_name == df_dim_product.name)
            & (df_src.product_category == df_dim_product.category),
            "inner",
        )
        .join(
            df_dim_store,
            (df_src.store_name == df_dim_store.name)
            & (df_src.store_location == df_dim_store.location),
            "inner",
        )
        .join(df_dim_supplier, df_src.supplier_email == df_dim_supplier.email, "inner")
        .select(
            df_dim_customer["customer_id"],
            df_dim_seller["seller_id"],
            df_dim_product["product_id"],
            df_dim_store["store_id"],
            df_dim_supplier["supplier_id"],
            col("sale_date"),
            col("sale_quantity").alias("quantity"),
            col("sale_total_price").alias("total_price"),
            col("product_quantity"),
        )
        .withColumn("sale_id", monotonically_increasing_id())
    )

    return {
        "dim_seller": df_dim_seller,
        "dim_product": df_dim_product,
        "dim_customer": df_dim_customer,
        "dim_store": df_dim_store,
        "dim_supplier": df_dim_supplier,
        "fact_sales": df_fact_sales,
    }


def save_to_postgres(df, table_name):
    df.write.format("jdbc").option("url", DB_URL).option("dbtable", table_name).option(
        "user", POSTGRES_USER
    ).option("password", POSTGRES_PASSWORD).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "overwrite"
    ).save()


def main():
    spark = create_spark_session()
    try:
        dataframes = process_data(spark)
        for table_name, df in dataframes.items():
            save_to_postgres(df, table_name)
        print(f"{datetime.now()} - Data update completed successfully")
    except Exception as e:
        print(f"Error during data update: {str(e)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
