from pyspark.sql import SparkSession

query = """
WITH streak_started AS (
    SELECT
		actor_id,
		actor,
		year,
		quality_class,
		is_active,
		LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY year) <> quality_class
			OR LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY year) IS NULL AS did_quality_class_change,
		LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY year) <> is_active
			OR LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY year) IS NULL AS did_is_active_change
    FROM actors
),
streak_identified AS (
	SELECT
		actor_id,
		actor,
		year,
		quality_class,
		is_active,
		SUM(CASE WHEN did_quality_class_change OR did_is_active_change THEN 1 ELSE 0 END)
            OVER (PARTITION BY actor_id ORDER BY year) as streak_identifier
	FROM streak_started
),
aggregated AS (
	SELECT
		actor_id,
		actor,
		quality_class,
		is_active,
		streak_identifier,
		MIN(year) AS start_date,
		MAX(year) AS end_date
	FROM streak_identified
	GROUP BY
		actor_id,
		actor,
		quality_class,
		is_active,
		streak_identifier
)

SELECT
	actor_id,
	actor,
	quality_class,
	is_active,
	start_date,
	end_date
FROM aggregated

"""

def do_actors_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_scd") \
      .getOrCreate()
    output_df = do_actors_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")