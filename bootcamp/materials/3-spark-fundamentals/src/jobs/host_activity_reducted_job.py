from pyspark.sql import SparkSession

query = """
WITH today_data AS (
    SELECT
        host,
        DATE(DATE_TRUNC('month', CAST(event_time AS TIMESTAMP))) AS month,
        ARRAY_AGG(DISTINCT DATE(CAST(event_time AS TIMESTAMP))) AS daily_hits_datelist,
        ARRAY_AGG(DISTINCT user_id) AS daily_unique_visitors_list
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01') -- Parameterized date
      AND host IS NOT NULL
      AND user_id IS NOT NULL
    GROUP BY
        host,
        month
)
SELECT
    host,
    month,
    daily_hits_datelist,
    daily_unique_visitors_list
FROM today_data
"""

def do_events_reducted_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("events")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("events_reducted") \
      .getOrCreate()
    output_df = do_events_reducted_transformation(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("events_reducted")