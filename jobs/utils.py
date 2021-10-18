from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import DoubleType, IntegerType, StringType
from datetime import datetime, timedelta
from typing import List
import pandas as pd
import re
from jobs.city_to_state import city_to_state_dict


@udf(StringType())
def city_to_state(city: str) -> str:
    return city_to_state_dict.get(city)


def find_src_with_largest_version(src_list: List[str]) -> str:
    version_dict = {
        int(re.findall("\\d+", src)[0]): src for src in src_list}
    max_version = max(version_dict.keys())
    return version_dict.get(max_version)


@pandas_udf(DoubleType())
def get_login_rate(x: pd.Series) -> float:
    deltas = datetime.now().date() - x
    return (deltas <= timedelta(days=90)).mean()


@pandas_udf(IntegerType())
def count_distinct(x: pd.Series) -> int:
    return len(x.value_counts())
