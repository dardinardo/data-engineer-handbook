-- CREATE TABLE web_events_dashboard AS 
WITH combined AS (
	SELECT
		COALESCE(d.browser_type, 'N/A') AS browser_type,
		COALESCE(d.os_type, 'N/A') AS os_type,
		we.*,
		CASE
			WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
			WHEN referrer LIKE '%eczachly%' THEN 'On Site'
			WHEN referrer LIKE '%dataengineer.io%' THEN 'On Site'
			WHEN referrer LIKE '%t.co%' THEN 'Twitter'
			WHEN referrer LIKE '%linkedin%' THEN 'LinkedIn'
			WHEN referrer LIKE '%instagram%' THEN 'Instagram'
			WHEN referrer IS NULL THEN 'Direct'
			ELSE 'Other'
		END AS referred_mapped
	FROM events AS we
	JOIN devices AS d
		ON we.device_id = d.device_id
)
SELECT
	COALESCE(referred_mapped, '(overall)') AS referrer,
	COALESCE(browser_type, '(overall)') AS browser_type,
	COALESCE(os_type, '(overall)') AS os_type,
	COUNT(1) AS number_of_site_hits,
	COUNT(CASE WHEN url = '/signup' THEN 1 END) AS number_of_signup_visits,
	COUNT(CASE WHEN url = '/contact' THEN 1 END) AS number_of_contact_visits,
	COUNT(CASE WHEN url = '/login' THEN 1 END) AS number_of_login_visits,
	CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL)/COUNT(1) AS pct_visited_signup
FROM combined
GROUP BY GROUPING SETS (
	(referred_mapped, browser_type, os_type),
	(os_type),
	(browser_type),
	(referred_mapped),
	()
)
HAVING COUNT(1) > 100
ORDER BY CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL)/COUNT(1) DESC

---------------------------------------------------------------------------------

WITH combined AS (
	SELECT
		COALESCE(d.browser_type, 'N/A') AS browser_type,
		COALESCE(d.os_type, 'N/A') AS os_type,
		we.*,
		CASE
			WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
			WHEN referrer LIKE '%eczachly%' THEN 'On Site'
			WHEN referrer LIKE '%dataengineer.io%' THEN 'On Site'
			WHEN referrer LIKE '%t.co%' THEN 'Twitter'
			WHEN referrer LIKE '%linkedin%' THEN 'LinkedIn'
			WHEN referrer LIKE '%instagram%' THEN 'Instagram'
			WHEN referrer IS NULL THEN 'Direct'
			ELSE 'Other'
		END AS referred_mapped
	FROM (SELECT DISTINCT * FROM events) AS we
	JOIN (SELECT DISTINCT * FROM devices) AS d
		ON we.device_id = d.device_id
),
aggregated AS (
SELECT
	c1.user_id,
	c1.url AS to_url,
	c2.url AS from_url,
	MIN(CAST(c1.event_time AS TIMESTAMP) - CAST(c2.event_time AS TIMESTAMP)) AS duration
FROM combined AS c1
JOIN combined AS c2
	ON c1.user_id = c2.user_id
	AND DATE(c1.event_time) = DATE(c2.event_time)
	AND c1.event_time > c2.event_time
-- WHERE c1.user_id = 511729477846131500
GROUP BY c1.user_id, c1.url, c2.url
-- LIMIT 100
-- ORDER BY c1.event_time 
)

SELECT
	to_url,
	from_url,
	count(1) AS number_of_users,
	MIN(duration) AS min_duration,
	MAX(duration) AS max_duration,
	AVG(duration) AS avg_duration
FROM aggregated
GROUP BY
	to_url,
	from_url
HAVING COUNT(1) > 50
ORDER BY COUNT(1) DESC
LIMIT 100