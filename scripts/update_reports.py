# update_reports.py
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

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


def check_for_changes(spark):
    # Проверяем, какие данные изменились за последние 24 часа
    # Можно использовать timestamp последнего обновления или checksum

    # Пример для таблицы фактов:
    last_update = (
        spark.read.format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "(SELECT MAX(updated_at) FROM fact_table) as t")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .load()
    )

    return last_update.collect()[0][0] > (datetime.now() - timedelta(days=1))


def update_reports(spark):
    # Ваша логика для обновления отчетов
    # Только для измененных данных
    pass


def main():
    spark = create_spark_session()
    try:
        if check_for_changes(spark):
            update_reports(spark)
            print(f"{datetime.now()} - Reports updated")
        else:
            print(f"{datetime.now()} - No changes detected")
    except Exception as e:
        print(f"Error during report update: {str(e)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
