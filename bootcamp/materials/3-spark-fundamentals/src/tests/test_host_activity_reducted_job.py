from chispa.dataframe_comparer import *
import datetime
from ..jobs.host_activity_reducted_job import do_events_reducted_transformation
from collections import namedtuple
Event = namedtuple("Event", "url referrer user_id device_id host event_time")
EventReducted = namedtuple("EventReducted", "host month daily_hits_datelist daily_unique_visitors_list")


def test_scd_generation(spark):
    source_data = [
        Event('/?author=12', None, '15049317011116400000', '8300763538851936000', 'www.zachwilson.tech', '2023-01-01 13:29:05.112000'),
        Event('/?author=13', None, '15049317011116400000', '8300763538851936000', 'www.zachwilson.tech', '2023-01-01 13:29:06.444000'),
        Event('/sitemap_index.xml', None, '16899212529494500000', '16255127598137100000', 'www.zachwilson.tech', '2023-01-01 00:06:50.079000'),
        Event('/', None, '17375809595660200000', '10598707916011500000', 'www.zachwilson.tech', '2023-01-01 00:20:03.261000'),
        Event('/', 'http://admin.zachwilson.tech', '12016633734290000000', '14616014151941600000', 'admin.zachwilson.tech', '2023-01-01 00:22:11.594000')
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_events_reducted_transformation(spark, source_df)
    expected_data = [
        EventReducted('admin.zachwilson.tech', (datetime.date(2023, 1, 1)), [(datetime.date(2023, 1, 1))], ['12016633734290000000']),
        EventReducted('www.zachwilson.tech', (datetime.date(2023, 1, 1)), [(datetime.date(2023, 1, 1))], ['16899212529494500000', '15049317011116400000', '17375809595660200000'])
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)