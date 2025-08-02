-- # Week 2 Fact Data Modeling
-- The homework this week will be using the `devices` and `events` dataset

-- Construct the following eight queries:

-- - A query to deduplicate `game_details` from Day 1 so there's no duplicates

WITH all_game_details AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS row_num
	FROM 
		game_details
)
SELECT
	*
FROM
	all_game_details
WHERE
	row_num = 1

-- - A DDL for an `user_devices_cumulated` table that has:
--   - a `device_activity_datelist` which tracks a users active days by `browser_type`
--   - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
--     - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

-- I decided to use `browser_type` column as shown below:

CREATE TABLE users_devices_cumulated (
    user_id TEXT,
	browser_type TEXT,
    device_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);

-- - A cumulative query to generate `device_activity_datelist` from `events`

INSERT INTO users_devices_cumulated
WITH all_devices AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY device_id) AS row_num
	FROM 
		devices
),
deduped_devices AS (
SELECT
	*
FROM
	all_devices
WHERE
	row_num = 1
),
all_events AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY user_id, event_time) AS row_num
	FROM 
		events
	WHERE
		user_id IS NOT NULL AND device_id IS NOT NULL
),
deduped_events AS (
SELECT
	*
FROM
	all_events
WHERE
	row_num = 1
),
yesterday AS (
    SELECT
        *
    FROM users_devices_cumulated
    WHERE date = DATE('2022-12-31') --2022-12-31, 2023-01-01, ..., 2023-01-30
),
today AS (
    SELECT
        CAST(e.user_id AS TEXT) AS user_id,
		d.browser_type,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
	FROM deduped_events AS e
	LEFT JOIN deduped_devices AS d ON e.device_id = d.device_id
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01') --2023-01-01, 2023-01-02, ..., 2023-01-31
    GROUP BY e.user_id, d.browser_type, DATE(CAST(e.event_time AS TIMESTAMP))
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.browser_type, y.browser_type) AS browser_type,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]::DATE[]
        WHEN t.date_active IS NULL THEN y.device_activity_datelist
        ELSE ARRAY[t.date_active] || y.device_activity_datelist
    END AS device_activity_datelist,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id
	AND t.browser_type = y.browser_type;

-- - A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

WITH users AS (
    SELECT
        *
    FROM
        users_devices_cumulated
    WHERE date = DATE('2023-01-31')
),

series AS (
    SELECT
        *
    FROM
        generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
),

place_holder_ints AS (
    SELECT
        CASE
            WHEN device_activity_datelist @> ARRAY[DATE(series_date)] THEN POW(2, 32 - (date - DATE(series_date)))
            ELSE 0
        END AS placeholder_int_value,
        *
    FROM users
    CROSS JOIN series
)

SELECT
    user_id,
	date,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
FROM place_holder_ints
GROUP BY
	user_id,
	date;

-- - A DDL for `hosts_cumulated` table 
--   - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity

CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (host, date)
);
  
-- - The incremental query to generate `host_activity_datelist`

INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM hosts_cumulated
    WHERE date = DATE('2022-12-31') --2022-12-31, 2023-01-01, ..., 2023-01-30
),

today AS (
    SELECT
        CAST(host AS TEXT) AS host,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01') --2023-01-01, 2023-01-02, ..., 2023-01-31
        AND host IS NOT NULL
    GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
)

SELECT
    COALESCE(t.host, y.host) AS host,
    CASE
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]::DATE[]
        WHEN t.date_active IS NULL THEN y.host_activity_datelist
        ELSE ARRAY[t.date_active] || y.host_activity_datelist
    END AS host_activity_datelist,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
    ON t.host = y.host;
	
-- - A monthly, reduced fact table DDL `host_activity_reduced`
--    - month
--    - host
--    - hit_array - think COUNT(1)
--    - unique_visitors array -  think COUNT(DISTINCT user_id)

CREATE TABLE host_activity_reduced (
    host TEXT,
    month DATE,
    hits_datelist DATE[],
    unique_visitors_list TEXT[],
    PRIMARY KEY (host, month)
);

-- - An incremental query that loads `host_activity_reduced`
--   - day-by-day

INSERT INTO host_activity_reduced
WITH today_data AS (
    SELECT
        host,
        DATE_TRUNC('month', CAST(event_time AS TIMESTAMP))::DATE AS month,
        ARRAY_AGG(DISTINCT DATE(CAST(event_time AS TIMESTAMP))) AS daily_hits_datelist,
        ARRAY_AGG(DISTINCT CAST(user_id AS TEXT)) AS daily_unique_visitors_list
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-05') -- Parameterized date
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
ON CONFLICT (host, month)
DO UPDATE SET
    hits_datelist = EXCLUDED.hits_datelist || host_activity_reduced.hits_datelist,
    -- Merge the existing and new user arrays, unnest them to rows, select distinct IDs, and re-aggregate into a clean array
    unique_visitors_list = (
        SELECT ARRAY_AGG(DISTINCT visitor_id)
        FROM unnest(EXCLUDED.unique_visitors_list || host_activity_reduced.unique_visitors_list) AS visitor_id
    );
