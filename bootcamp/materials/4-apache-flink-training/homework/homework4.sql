-- # Homework

-- - Create a Flink job that sessionizes the input data by IP address and host

-- The Flink Job is in the homework_job.py file

-- - Use a 5 minute gap
-- - Answer these questions
--   - What is the average number of web events of a session from a user on Tech Creator?

SELECT
	AVG(num_hits) AS avg_session_web_events
FROM processed_session_events_aggregated
-- result:
-- 3.7261263628004729

--   - Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)

SELECT
	host,
	AVG(num_hits) AS avg_session_host_web_events
FROM processed_session_events_aggregated_source
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host

-- results:
-- "lulu.techcreator.io"	2.5000000000000000
-- "zachwilson.techcreator.io"	2.6363636363636364