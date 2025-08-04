-- CREATE TABLE users_cumulated (
--     user_id TEXT,
--     -- The list of dates in the past where the user was active
--     dates_active DATE[],
--     -- The current date for the user
--     date DATE,
--     PRIMARY KEY (user_id, date)
-- );

INSERT INTO users_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM users_cumulated
    WHERE date = DATE('2023-01-30') --2022-12-31, 2023-01-01, ..., 2023-01-30
),

today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31') --2023-01-01, 2023-01-02, ..., 2023-01-31
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)

SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE
        WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]::DATE[]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY[t.date_active] || y.dates_active
    END AS dates_active,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id;

--------------------------------------------------
WITH users AS (
    SELECT
        *
    FROM
        users_cumulated
    WHERE date = DATE('2023-01-31')
),

series AS (
    SELECT
        *
    FROM
        generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
),
-- @> è l’operatore di "contiene" per array, dates_active @> ARRAY[DATE(series_date)] → l'utente era attivo nel giorno series_date?
-- POW(2, 32 - (date - DATE(series_date))) mi restituisce il numero in potenza di 2(lo castiamo dopo a bit 32) della differenza tra i giorni dove è stato attivo
-- per esempio se sono stato attivo 2 giorni fa e 4 giorni fa -> 0101000... ma in potenza di due, quindi 1342177280
place_holder_ints AS (
    SELECT
        CASE
            WHEN dates_active @> ARRAY[DATE(series_date)] THEN POW(2, 32 - (date - DATE(series_date)))
            ELSE 0
        END AS placeholder_int_value,
        *
    FROM users
    CROSS JOIN series
    -- WHERE user_id = '45836745783658' --esempio, non un user_id reale
)

SELECT
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
FROM place_holder_ints
GROUP BY user_id;

------------------------------------------------
SELECT
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
    BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active, -- bitwise and
    BIT_COUNT(CAST('00000001111111000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly2_active, -- bitwise and
    BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active, -- bitwise and
FROM place_holder_ints
GROUP BY user_id;