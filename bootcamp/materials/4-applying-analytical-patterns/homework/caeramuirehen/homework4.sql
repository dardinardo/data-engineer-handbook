-- # Week 4 Applying Analytical Patterns
-- The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1

-- - A query that does state change tracking for `players`
--   - A player entering the league should be `New`
--   - A player leaving the league should be `Retired`
--   - A player staying in the league should be `Continued Playing`
--   - A player that comes out of retirement should be `Returned from Retirement`
--   - A player that stays out of the league should be `Stayed Retired`

-- I choose to use an incremental strategy, iterating on differente season the query is able to track state changes in players activity
WITH yesterday AS (
	SELECT * FROM player_growth_accounting
	WHERE season = 1998 -- 1995, 1996, 1997, 1998, 1999, ..., 2020
),
today AS (
	SELECT
		player_name,
		current_season --today_date
	FROM players
	WHERE current_season = 1999 -- 1996, 1997, 1998, 1999, ..., 2021
		AND player_name IS NOT NULL
		-- AND player_name = 'Michael Jordan'
		AND is_active IS TRUE
	GROUP BY
		player_name,
		current_season
)

SELECT
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(y.first_active_season, t.current_season) AS first_active_season,
    COALESCE(t.current_season, y.last_active_season) AS last_active_season,
	CASE
		WHEN y.player_name IS NULL AND t.player_name IS NOT NULL THEN 'New'
		WHEN y.last_active_season = t.current_season - 1 THEN 'Continued Playing'
		WHEN y.last_active_season < t.current_season - 1 THEN 'Returned from Retirement'
		WHEN t.current_season IS NULL AND y.last_active_season = y.season THEN 'Retired'
		WHEN t.current_season IS NULL AND y.last_active_season < y.season THEN 'Stayed Retired'
		ELSE 'Unknown'
	END AS seasons_active_state,
	CASE
		WHEN t.player_name IS NOT NULL THEN ARRAY [t.current_season] || COALESCE(y.seasons_active, ARRAY []::INTEGER[])
		ELSE ARRAY []::INTEGER[] || COALESCE(y.seasons_active, ARRAY []::INTEGER[])
	END AS seasons_active,
    COALESCE(t.current_season, y.season + 1) as season
FROM today t
FULL OUTER JOIN yesterday y on t.player_name = y.player_name
WHERE COALESCE(t.player_name, y.player_name) = 'Michael Jordan'
  
-- - A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
--   - Aggregate this dataset along the following dimensions
--     - player and team
--       - Answer questions like who scored the most points playing for one team?
--     - player and season
--       - Answer questions like who scored the most points in one season?
--     - team
--       - Answer questions like which team has won the most games?

WITH start_data AS (
	SELECT
		gd.team_city,
		gd.player_name,
		g.season,
		gd.pts,
		CASE 
            WHEN gd.team_id = g.team_id_home THEN g.home_team_wins
            ELSE 1 - g.home_team_wins
        END AS team_win
	FROM game_details AS gd
	LEFT JOIN games AS g
		ON g.game_id = gd.game_id
	WHERE gd.pts IS NOT NULL
),
aggregations AS (
	SELECT
		COALESCE(team_city, 'Overall') AS team_city,
		COALESCE(player_name, 'Overall') AS player_name,
		COALESCE(CAST(season AS TEXT), 'Overall') AS season,
		SUM(pts) AS total_pts,
		SUM(team_win) AS games_won
	FROM
		start_data
	GROUP BY GROUPING SETS (
		(player_name, team_city),
		(player_name, season),
		(team_city)
	)
),
player_most_points AS (
	SELECT
		'Player scored the most points playing for one team' AS metric,
		player_name,
		team_city,
		season,
		total_pts AS total_pts,
		-1 AS games_won
	FROM
		aggregations
	WHERE
		season = 'Overall'
		AND player_name <> 'Overall'
		AND team_city <> 'Overall'
	ORDER by total_pts DESC
	LIMIT 1
),
season_most_points AS (
	SELECT
		'Player scored the most points in one season' AS metric,
		player_name,
		team_city,
		season,
		total_pts AS total_pts,
		-1 AS games_won
	FROM
		aggregations
	WHERE
		team_city = 'Overall'
		AND player_name <> 'Overall'
		AND season <> 'Overall'
	ORDER by total_pts DESC
	LIMIT 1
),
team_most_game_won AS (
	SELECT
		'Team has won the most games' AS metric,
		player_name,
		team_city,
		season,
		-1 AS total_pts,
		games_won AS games_won
	FROM
		aggregations
	WHERE
		player_name = 'Overall'
		AND season = 'Overall'
		AND team_city <> 'Overall'
	ORDER by games_won DESC
	LIMIT 1
)
SELECT * FROM player_most_points
UNION ALL
SELECT * FROM season_most_points
UNION ALL
SELECT * FROM team_most_game_won

-- - A query that uses window functions on `game_details` to find out the following things:
--   - What is the most games a team has won in a 90 game stretch? 
--   - How many games in a row did LeBron James score over 10 points a game?

WITH start_data AS (
	SELECT
		-- gd.*,
		gd.game_id,
		gd.team_id,
		gd.team_abbreviation,
		gd.team_city,
		gd.player_id,
		gd.player_name,
		gd.pts,
		g.season,
		g.game_date_est,
		CASE 
            WHEN gd.team_id = g.team_id_home THEN g.home_team_wins
            ELSE 1 - g.home_team_wins
        END AS team_win
	FROM game_details AS gd
	LEFT JOIN games AS g
		ON g.game_id = gd.game_id
),
team_win_rolling AS (
    SELECT
        team_id,
        game_date_est,
        SUM(team_win) OVER (
            PARTITION BY team_id 
            ORDER BY game_date_est 
            ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING
        ) AS team_90_game_wins
    FROM start_data
),
lebron_games AS (
    SELECT
        game_date_est,
        pts,
        ROW_NUMBER() OVER (ORDER BY game_date_est) AS rn,
        ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY game_date_est) 
            - ROW_NUMBER() OVER (PARTITION BY player_name, (CASE WHEN pts > 10 THEN 1 ELSE 0 END) ORDER BY game_date_est) AS grp
    FROM start_data
    WHERE player_name = 'LeBron James' AND pts > 10
),
lebron_streaks AS (
    SELECT COUNT(*) AS streak_length
    FROM lebron_games
    GROUP BY grp
),
max_lb_streak AS (
    SELECT MAX(streak_length) AS max_consecutive_games_over_10_pts
    FROM lebron_streaks
),
max_team_wins AS (
    SELECT MAX(team_90_game_wins) AS max_wins_in_90_game_stretch
    FROM team_win_rolling
)
SELECT 
    max_team_wins.max_wins_in_90_game_stretch,
    max_lb_streak.max_consecutive_games_over_10_pts
FROM max_team_wins
CROSS JOIN max_lb_streak;

-- Please add these queries into a folder `homework/<discord-username>`