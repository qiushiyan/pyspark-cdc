from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from functools import reduce
import os
from src.downloader import Downloader
from src.actions import delete_, insert_, update_


class Job:
    def __init__(self):
        self.data_dir = Path().resolve() / "src/data"
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

    def get_src(self):
        if len(os.listdir("src/data")) == 0:
            d = Downloader
            d.download_data()
        files = [f.name for f in self.data_dir.iterdir()]
        self.main_data_src = "src/data/LOAD00000001.csv"
        self.cdc_data_src = ["src/data/" +
                             f for f in files if f != self.main_data_src]

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

    def collect_cdc(self) -> DataFrame:
        cdc_data = reduce(
            lambda x, y: x.union(y),
            [self.read_df(f, type="cdc") for f in self.cdc_data_src]
        )
        return cdc_data

    def collect_main(self) -> DataFrame:
        main_data = self.read_df(self.main_data_src, type="main")
        return main_data

    def update_main_data(self):
        cdc_data = self.collect_cdc()
        main_data = self.collect_main()
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


if __name__ == "__main__":
    job = Job()
    job.get_src()
    main_data_updated = job.update_main_data()
    main_data_updated.show(20, False)
