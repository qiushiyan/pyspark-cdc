from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from datetime import date


def update_(df: DataFrame, user_id: int, name: str, city: str, last_login: date) -> DataFrame:
    return df.withColumn(
        "name", when(col("user_id") == user_id, name).otherwise(col("name"))
    ).withColumn(
        "city", when(col("user_id") == user_id, city).otherwise(col("city"))
    ).withColumn(
        "last_login", when(col("user_id") == user_id,
                           last_login).otherwise(col("last_login"))
    )


def delete_(df: DataFrame, user_id: str) -> DataFrame:
    return df.where(f"user_id != {user_id}")


def insert_(df: DataFrame, **kwargs) -> DataFrame:
    spark = kwargs.get("spark")
    del kwargs["spark"]
    new_row = spark.createDataFrame(
        [kwargs], schema="`user_id` INT, `name` STRING, `city` STRING, last_login DATE")
    return df.union(new_row)
