from chispa.dataframe_comparer import *
from ..jobs.actor_scd_job import do_actors_scd_transformation
from collections import namedtuple
ActorYear = namedtuple("ActorYear", "actor_id actor year quality_class is_active,")
ActorScd = namedtuple("ActorScd", "actor_id actor quality_class is_active start_date end_date")


def test_scd_generation(spark):
    source_data = [
        ActorYear("nm0000002", "Lauren Bacall", 1980, 'Bad', True),
        ActorYear("nm0000004", "John Belushi", 1976, 'Bad', False),
        ActorYear("nm0000004", "John Belushi", 1977, 'Bad', False),
        ActorYear("nm0000004", "John Belushi", 1978, 'Average', True),
        ActorYear("nm0000004", "John Belushi", 1979, 'Bad', True),
        ActorYear("nm0000004", "John Belushi", 1980, 'Good', True)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("nm0000002", "Lauren Bacall", 'Bad', True, 1980, 1980),
        ActorScd("nm0000004", "John Belushi", 'Bad', False, 1976, 1977),
        ActorScd("nm0000004", "John Belushi", 'Average', True, 1978, 1978),
        ActorScd("nm0000004", "John Belushi", 'Bad', True, 1979, 1979),
        ActorScd("nm0000004", "John Belushi", 'Good', True, 1980, 1980)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)