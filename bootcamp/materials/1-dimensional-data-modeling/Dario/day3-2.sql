SELECT
    type,
    COUNT(1)
FROM vertices
GROUP BY 1
-- team, 30; player, 1496, game 9384

SELECT
    v.properties->>'player_name',
    MAX(CAST(e.properties->>'pts' AS INTEGER))
FROM
    vertices v
JOIN edges e
    ON v.identifier = e.subject_identifier
    AND v.type = e.subject_type
GROUP BY 1
ORDER BY 2 DESC


INSERT INTO edges
SELECT
    player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    game_id AS object_identifier,
    'game'::vertex_type AS object_type,
    'plays_in'::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation,
    ) AS properties
FROM game_details;

