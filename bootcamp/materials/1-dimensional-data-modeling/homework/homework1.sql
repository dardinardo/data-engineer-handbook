-- # Dimensional Data Modeling - Week 1

-- This week's assignment involves working with the `actor_films` dataset. Your task is to construct a series of SQL queries and table definitions that will allow us to model the actor_films dataset in a way that facilitates efficient analysis. This involves creating new tables, defining data types, and writing queries to populate these tables with data from the actor_films dataset

-- ## Dataset Overview
-- The `actor_films` dataset contains the following fields:

-- - `actor`: The name of the actor.
-- - `actorid`: A unique identifier for each actor.
-- - `film`: The name of the film.
-- - `year`: The year the film was released.
-- - `votes`: The number of votes the film received.
-- - `rating`: The rating of the film.
-- - `filmid`: A unique identifier for each film.

-- The primary key for this dataset is (`actor_id`, `film_id`).

-- ## Assignment Tasks

-- 1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
--     - `films`: An array of `struct` with the following fields:
-- 		- film: The name of the film.
-- 		- votes: The number of votes the film received.
-- 		- rating: The rating of the film.
-- 		- filmid: A unique identifier for each film.

--     - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
-- 		- `star`: Average rating > 8.
-- 		- `good`: Average rating > 7 and ≤ 8.
-- 		- `average`: Average rating > 6 and ≤ 7.
-- 		- `bad`: Average rating ≤ 6.
--     - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).

CREATE TYPE films AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT,
    year INTEGER
)

CREATE TYPE quality_class AS
    ENUM ('star',
          'good',
          'average',
          'bad'
        );

CREATE TABLE actors (
    actor TEXT,
    actor_id TEXT,
    year INTEGER,
    films films[],
	quality_class quality_class,
	is_active BOOLEAN,
	PRIMARY KEY(actor_id, year)
);

-- 2. **Cumulative table generation query:** Write a query that populates the `actors` table one year at a time.

WITH last_year AS (
    SELECT
		*
	FROM
		public.actors
	WHERE year = 2000

), this_year AS (
    SELECT
        actorid,
        actor,
		year,
        ARRAY_AGG(ROW(film, votes, rating, filmid, year)::films) AS films,
        AVG(rating) AS rating
    FROM public.actor_films
	WHERE year = 2001
    GROUP BY actorid, actor, year
)

INSERT INTO actors
SELECT
	COALESCE(ly.actor, ty.actor) as actor,
	COALESCE(ly.actor_id, ty.actorid) AS actor_id,
	COALESCE(ty.year, ly.year::integer+1) as year,
	CASE
		WHEN ly.films IS NULL THEN ty.films
		ELSE (CASE WHEN ty.films IS NOT NULL THEN ly.films || ty.films ELSE ly.films END)
	END AS films,
	CASE
		WHEN ty.rating IS NOT NULL THEN (
			CASE	
				WHEN ty.rating > 8 THEN 'star'
				WHEN ty.rating > 7 AND ty.rating <= 8 THEN 'good'
				WHEN ty.rating > 6 AND ty.rating <= 7 THEN 'average'
				WHEN ty.rating <= 6 THEN 'bad'
			END
		)::quality_class
		ELSE ly.quality_class
	END AS quality_class,
	COALESCE(ty.actorid IS NOT NULL, FALSE) AS is_active
FROM last_year ly
FULL OUTER JOIN this_year ty
    ON ly.actor_id = ty.actorid;

-- 3. **DDL for `actors_history_scd` table:** Create a DDL for an `actors_history_scd` table with the following features:
--     - Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
--     - Tracks `quality_class` and `is_active` status for each actor in the `actors` table.

CREATE TABLE actors_history_scd (
    actor_id TEXT,
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date DATE,
    end_date DATE,
    PRIMARY KEY(actor_id, start_date)
);

-- 4. **Backfill query for `actors_history_scd`:** Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.

INSERT INTO actors_history_scd
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

-- 5. **Incremental query for `actors_history_scd`:** Write an "incremental" query that combines the previous year's SCD data with new incoming data from the `actors` table.

-- CREATE TYPE scd_type AS (
-- 	quality_class quality_class,
-- 	is_active boolean,
-- 	start_date INTEGER,
-- 	end_date INTEGER
-- );

INSERT INTO actors_history_scd
WITH last_season_scd AS (
    SELECT
        *
    FROM actors_history_scd
    WHERE
        start_date = 1969
        AND end_date = 1969
),

historical_scd AS (
SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
FROM actors_history_scd
WHERE 
    start_date = 1969
    AND end_date < 1969
),

this_season_data AS (
    SELECT
        *
    FROM actors
    WHERE
        year = 1970
),

unchanged_records AS (
    SELECT
        ts.actor_id,
        ts.actor,
        ts.quality_class,
        ts.is_active,
        ls.start_date,
        ts.year AS end_date
FROM this_season_data ts
JOIN last_season_scd ls
ON ls.actor_id = ts.actor_id
    WHERE ts.quality_class = ls.quality_class
    AND ts.is_active = ls.is_active
),

changed_records AS (
SELECT
    ts.actor_id,
    ts.actor,
    UNNEST(ARRAY[
        ROW(
            ls.quality_class,
            ls.is_active,
            ls.start_date,
            ls.end_date
            )::scd_type,
        ROW(
            ts.quality_class,
            ts.is_active,
            ts.year,
            ts.year
            )::scd_type
    ]) AS records
FROM this_season_data ts
LEFT JOIN last_season_scd ls
ON ls.actor_id = ts.actor_id
    WHERE (ts.quality_class <> ls.quality_class
    OR ts.is_active <> ls.is_active)
),

unnested_changed_records AS (
    SELECT
        actor_id,
        actor,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_date,
        (records::scd_type).end_date
    FROM changed_records
),

new_records AS (
    SELECT
        ts.actor_id,
        ts.actor,
        ts.quality_class,
        ts.is_active,
        ts.year AS start_date,
        ts.year AS end_date
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
        ON ts.actor_id = ls.actor_id
    WHERE
        ls.actor_id IS NULL
)

SELECT
	*
FROM (
	SELECT *
	FROM historical_scd
	
	UNION ALL
	
	SELECT *
	FROM unchanged_records
	
	UNION ALL
	
	SELECT *
	FROM unnested_changed_records
	
	UNION ALL
	
	SELECT *
	FROM new_records
)
ON CONFLICT(actor_id, start_date)
DO UPDATE SET
    quality_class = EXCLUDED.quality_class,
	is_active = EXCLUDED.is_active,
	end_date = EXCLUDED.end_date;
