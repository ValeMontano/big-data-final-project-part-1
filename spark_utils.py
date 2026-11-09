from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class SparkUtils:

    def __init__(self, app_name):
        self._spark = (
            SparkSession.builder
            .appName(app_name)
            .getOrCreate()
        )

    @property
    def spark(self):
        return self._spark

    def describe_data(self, df):
        df.printSchema()
        print("Input records:", df.count())

    def clean_data(self, df):
        return (
            df.select("id", "subtype", "height")
            .dropna(subset=["id"])
            .dropDuplicates(["id"])
            .fillna({"subtype": "unknown"})
        )

    def add_height_group(self, df):
        return df.withColumn(
            "height_group",
            F.when(F.col("height").isNull(), "unknown")
            .when(F.col("height") < 10, "under_10m")
            .when(F.col("height") < 30, "10_to_under_30m")
            .otherwise("30m_or_more")
        )

    def summarize_data(self, df):
        return (
            df.groupBy("subtype", "height_group")
            .agg(
                F.count("*").alias("buildings"),
                F.avg("height").alias("avg_height")
            )
        )

    def save_data(self, df, output_path):
        (
            df.write
            .mode("overwrite")
            .partitionBy("height_group")
            .parquet(output_path)
        )
