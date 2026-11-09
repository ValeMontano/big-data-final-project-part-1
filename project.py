import argparse

from spark_utils import SparkUtils


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    su = SparkUtils("Final Project - Valentina")

    df = su.spark.read.parquet(args.input)

    su.describe_data(df)

    clean_df = su.clean_data(df)
    grouped_df = su.add_height_group(clean_df)
    result_df = su.summarize_data(grouped_df)

    su.save_data(result_df, args.output)

    print("Output:", args.output)

    su.spark.stop()


if __name__ == "__main__":
    main()
