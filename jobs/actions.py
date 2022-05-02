from datetime import date

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def update_(
    df: DataFrame, user_id: int, name: str, city: str, last_login: date
) -> DataFrame:
    return (
        df.withColumn(
            "name", F.when(F.col("user_id") == user_id, name).otherwise(F.col("name"))
        )
        .withColumn(
            "city", F.when(F.col("user_id") == user_id, city).otherwise(F.col("city"))
        )
        .withColumn(
            "last_login",
            F.when(F.col("user_id") == user_id, last_login).otherwise(
                F.col("last_login")
            ),
        )
    )


def delete_(df: DataFrame, user_id: str) -> DataFrame:
    return df.where(f"user_id != {user_id}")


def insert_(df: DataFrame, **kwargs) -> DataFrame:
    spark = kwargs.get("spark")
    del kwargs["spark"]
    new_row = spark.createDataFrame(
        [kwargs], schema="`user_id` INT, `name` STRING, `city` STRING, last_login DATE"
    )
    return df.union(new_row)
