from typing import Union, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import dense_rank, col, when
from pyspark.sql.window import Window
from pathlib import Path
from functools import reduce
import os
from jobs.downloader import Downloader
from jobs.actions import delete_, insert_, update_
from jobs.utils import city_to_state, find_src_with_largest_version, get_login_rate, count_distinct


class Job:
    def __init__(self):
        self.data_dir = Path().resolve() / "data"
        self.spark = SparkSession.builder.appName("cdc-pyspark").getOrCreate()
        self.actions = {
            "U": update_,
            "I": insert_,
            "D": delete_
        }
        self.schema = {
            "main": "`user_id` INT, `name` STRING, `city` STRING, `last_login` DATE",
            "cdc": "`action` STRING, `user_id` INT, `name` STRING, `city` STRING, `last_login` DATE"
        }

    def get_src(self, type: str) -> Union[str, List[str]]:
        data_dir = Path().resolve() / "data"
        if not data_dir.exists():
            data_dir.mkdir(exist_ok=True, parents=True)
        if len(os.listdir("data")) == 0:
            d = Downloader()
            d.download_data()
        files = ["data/" + f.name for f in self.data_dir.iterdir()]
        if type == "main":
            main_data_src = [f for f in files if "LOAD" in f]
            return main_data_src
        elif type == "cdc":
            cdc_data_src = [f for f in files if "LOAD" not in f]
            return cdc_data_src

    def rename_cols(self, df: DataFrame, type) -> DataFrame:
        if type == "main":
            df_renamed = (df
                          .withColumnRenamed("_c0", "user_id")
                          .withColumnRenamed("_c1", "name")
                          .withColumnRenamed("_c2", "city")
                          .withColumnRenamed("_c3", "last_login")
                          )
        elif type == "cdc":
            df_renamed = (df
                          .withColumnRenamed("_c0", "action")
                          .withColumnRenamed("_c1", "user_id")
                          .withColumnRenamed("_c2", "name")
                          .withColumnRenamed("_c3", "city")
                          .withColumnRenamed("_c4", "last_login")
                          )
        return df_renamed

    def read_df(self, src: str, type: str) -> DataFrame:
        schema = self.schema.get(type)
        df = self.spark.read.csv(src, schema=schema)
        df_renamed = self.rename_cols(df, type)
        return df_renamed

    def collect(self, type) -> DataFrame:
        """
        for cdc data, union all files
        for main data, read the file with largest version number
        """
        src_list = self.get_src(type)
        if type == "cdc":
            data = reduce(
                lambda x, y: x.union(y),
                [self.read_df(src, type=type) for src in src_list]
            )
        elif type == "main":
            src = find_src_with_largest_version(src_list)
            data = self.read_df(src, type=type)
        return data

    def update_main_data(self) -> DataFrame:
        cdc_data = self.collect("cdc")
        main_data = self.collect("main")
        for row in cdc_data.collect():
            action = row["action"]
            processor = self.actions.get(action)
            if action == "D":
                main_data = processor(main_data, user_id=row["user_id"])
            elif action == "I":
                main_data = processor(main_data,
                                      spark=self.spark,
                                      user_id=row["user_id"],
                                      name=row["name"],
                                      city=row["city"],
                                      last_login=row["last_login"])
            elif action == "U":
                main_data = processor(main_data,
                                      user_id=row["user_id"],
                                      name=row["name"],
                                      city=row["city"],
                                      last_login=row["last_login"])
        return main_data

    def prepare_main_data(self, main_data: DataFrame) -> DataFrame:
        city_window = Window().partitionBy("city")
        prepared_data = (main_data
                         .withColumn(
                             "city", when(col("city") == "Nashville-Davidson",
                                          "Nashville").otherwise(col("city"))
                         )
                         .withColumn(
                             "state", city_to_state(col("city"))
                         )
                         .withColumn(
                             "last_login_rank_by_city", dense_rank().over(
                                 city_window.orderBy(col("last_login").desc()))
                         ).withColumn(
                             "login_rate_last_3_months_by_city", get_login_rate(
                                 col("last_login")).over(city_window)
                         ).withColumn(
                             "users_by_city", count_distinct(
                                 col("user_id")).over(city_window)
                         ).orderBy(col("city"), col("last_login_rank_by_city").desc())
                         )
        return prepared_data


if __name__ == "__main__":
    job = Job()
    main_data_updated = job.update_main_data()
    main_data_prepared = job.prepare_main_data(main_data_updated)
    main_data_prepared.show(10, False)
    main_data_updated.write.csv("data/output", mode="overwrite")
