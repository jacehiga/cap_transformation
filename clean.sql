START TRANSACTION;

-- JSON MLB BOX SCORE INSERTS --

-- Insert Games --
INSERT INTO games (
  game_id,
  date,
  home_team_id,
  home_score,
  away_team_id,
  away_score
)
SELECT
  (raw_json->>'game_id')::BIGINT AS game_id,
  (raw_json->>'date')::DATE AS date,
  (raw_json->'home_team'->>'id')::BIGINT AS home_team_id,
  (raw_json->'final_score'->>(raw_json->'home_team'->>'id'))::INT AS home_score,
  (raw_json->'away_team'->>'id')::BIGINT AS away_team_id,
  (raw_json->'final_score'->>(raw_json->'away_team'->>'id'))::INT AS away_score
FROM json_mlb
ON CONFLICT (game_id) DO UPDATE
SET
  date = EXCLUDED.date,
  home_score = EXCLUDED.home_score,
  away_score = EXCLUDED.away_score;
  
  
 

-- Insert Players Table --
WITH dedup_players AS (
  SELECT DISTINCT ON (player_id)
    (batter ->> 'personId')::bigint AS player_id,
    batter ->> 'name' AS player_name
  FROM json_mlb,
       jsonb_array_elements(raw_json::jsonb -> 'away_batters') AS batter
  WHERE (batter ->> 'personId')::int != 0

  UNION

  SELECT DISTINCT ON (player_id)
    (batter ->> 'personId')::bigint AS player_id,
    batter ->> 'name' AS player_name
  FROM json_mlb,
       jsonb_array_elements(raw_json::jsonb -> 'home_batters') AS batter
  WHERE (batter ->> 'personId')::int != 0

  UNION

  SELECT DISTINCT ON (player_id)
    (pitcher ->> 'personId')::bigint AS player_id,
    pitcher ->> 'name' AS player_name
  FROM json_mlb,
       jsonb_array_elements(raw_json::jsonb -> 'away_pitchers') AS pitcher
  WHERE (pitcher ->> 'personId')::int != 0

  UNION

  SELECT DISTINCT ON (player_id)
    (pitcher ->> 'personId')::bigint AS player_id,
    pitcher ->> 'name' AS player_name
  FROM json_mlb,
       jsonb_array_elements(raw_json::jsonb -> 'home_pitchers') AS pitcher
  WHERE (pitcher ->> 'personId')::int != 0
)

INSERT INTO players (player_id, player_name)
SELECT * FROM dedup_players
ON CONFLICT (player_id) DO UPDATE
SET player_name = EXCLUDED.player_name;



-- Insert Sides Table --

-- Insert from away teams
INSERT INTO sides (game_id, team_id, side)
SELECT DISTINCT 
  (raw_json->>'game_id')::BIGINT AS game_id,
  (raw_json->'away_team'->>'id')::BIGINT AS team_id,
  'away' AS side
FROM json_mlb
WHERE raw_json->'away_team'->>'id' IS NOT NULL

UNION

-- Insert from home teams
SELECT DISTINCT 
  (raw_json->>'game_id')::BIGINT AS game_id,
  (raw_json->'home_team'->>'id')::BIGINT AS team_id,
  'home' AS side
FROM json_mlb
WHERE raw_json->'home_team'->>'id' IS NOT NULL
ON CONFLICT (game_id, team_id) DO NOTHING;




-- Insert Away Batters Box Scores --
WITH away_box_scores AS (
  SELECT
    (raw_json->>'game_id')::BIGINT AS game_id,
    (batter->>'personId')::BIGINT AS batter_id,
    (raw_json->'away_team'->>'id')::BIGINT AS team_id,
    batter->>'position' AS position,
    (batter->>'ab')::INT AS ab,
    (batter->>'h')::INT AS h,
    (batter->>'bb')::INT AS bb,
    (batter->>'r')::INT AS r,
    (batter->>'rbi')::INT AS rbi,
    (batter->>'k')::INT AS so,
    (batter->>'doubles')::INT AS double,
    (batter->>'triples')::INT AS triple,
    (batter->>'hr')::INT AS hr,
    (batter->>'sb')::INT AS sb
  FROM json_mlb,
       LATERAL jsonb_array_elements(raw_json->'away_batters') AS batter
  WHERE batter->>'personId' IS NOT NULL
    AND batter->>'personId' != '0'
    AND batter->>'position' IS NOT NULL
)
INSERT INTO batter_stats (
  game_id, batter_id, team_id, position,
  ab, h, bb, r, rbi, so, double, triple, hr, sb
)
SELECT * FROM away_box_scores
ON CONFLICT (game_id, batter_id) DO NOTHING;



-- Insert Home Batters Box Scores --
WITH home_box_scores AS (
  SELECT
    (raw_json->>'game_id')::BIGINT AS game_id,
    (batter->>'personId')::BIGINT AS batter_id,
    (raw_json->'home_team'->>'id')::BIGINT AS team_id,
    batter->>'position' AS position,
    (batter->>'ab')::INT AS ab,
    (batter->>'h')::INT AS h,
    (batter->>'bb')::INT AS bb,
    (batter->>'r')::INT AS r,
    (batter->>'rbi')::INT AS rbi,
    (batter->>'k')::INT AS so,
    (batter->>'doubles')::INT AS double,
    (batter->>'triples')::INT AS triple,
    (batter->>'hr')::INT AS hr,
    (batter->>'sb')::INT AS sb
  FROM json_mlb,
       LATERAL jsonb_array_elements(raw_json->'home_batters') AS batter
  WHERE batter->>'personId' IS NOT NULL
    AND batter->>'personId' != '0'
    AND batter->>'position' IS NOT NULL
)
INSERT INTO batter_stats (
  game_id, batter_id, team_id, position,
  ab, h, bb, r, rbi, so, double, triple, hr, sb
)
SELECT * FROM home_box_scores
ON CONFLICT (game_id, batter_id) DO NOTHING;





-- Insert Away Pitchers Box Scores --
WITH indexed_away_pitchers AS (
  SELECT
    (raw_json->>'game_id')::BIGINT AS game_id,
    (pitcher->>'personId')::BIGINT AS pitcher_id,
    (raw_json->'away_team'->>'id')::BIGINT AS team_id,
    CASE 
      WHEN row_number() OVER (PARTITION BY raw_json->>'game_id' ORDER BY ord) = 1 THEN 'SP'
      ELSE 'RP'
    END AS type,
    (pitcher->>'ip')::FLOAT AS ip,
    (pitcher->>'h')::INT AS h,
    (pitcher->>'r')::INT AS r,
    (pitcher->>'er')::INT AS er,
    (pitcher->>'bb')::INT AS bb,
    (pitcher->>'k')::INT AS so,
    (pitcher->>'hr')::INT AS hr,
    (pitcher->>'p')::BIGINT AS pitches,
    (pitcher->>'s')::BIGINT AS strikes,
    (pitcher->>'era')::FLOAT AS era
  FROM json_mlb,
       LATERAL jsonb_array_elements(raw_json->'away_pitchers') WITH ORDINALITY AS t(pitcher, ord)
  WHERE pitcher->>'personId' IS NOT NULL AND pitcher->>'personId' != '0'
)
INSERT INTO pitcher_stats (
  game_id, pitcher_id, team_id, type, ip, h, r, er, bb, so, hr, pitches, strikes, era
)
SELECT * FROM indexed_away_pitchers
ON CONFLICT DO NOTHING;



-- Insert Home Pitchers Box Scores --
WITH indexed_home_pitchers AS (
  SELECT
    (raw_json->>'game_id')::BIGINT AS game_id,
    (pitcher->>'personId')::BIGINT AS pitcher_id,
    (raw_json->'home_team'->>'id')::BIGINT AS team_id,
    CASE 
      WHEN row_number() OVER (PARTITION BY raw_json->>'game_id' ORDER BY ord) = 1 THEN 'SP'
      ELSE 'RP'
    END AS type,
    (pitcher->>'ip')::FLOAT AS ip,
    (pitcher->>'h')::INT AS h,
    (pitcher->>'r')::INT AS r,
    (pitcher->>'er')::INT AS er,
    (pitcher->>'bb')::INT AS bb,
    (pitcher->>'k')::INT AS so,
    (pitcher->>'hr')::INT AS hr,
    (pitcher->>'p')::BIGINT AS pitches,
    (pitcher->>'s')::BIGINT AS strikes,
    (pitcher->>'era')::FLOAT AS era
  FROM json_mlb,
       LATERAL jsonb_array_elements(raw_json->'home_pitchers') WITH ORDINALITY AS t(pitcher, ord)
  WHERE pitcher->>'personId' IS NOT NULL AND pitcher->>'personId' != '0'
)
INSERT INTO pitcher_stats (
  game_id, pitcher_id, team_id, type, ip, h, r, er, bb, so, hr, pitches, strikes, era
)
SELECT * FROM indexed_home_pitchers
ON CONFLICT DO NOTHING;


-- JSON MLB PREVIEW INSERTS --


-- Pitcher preview inserts

INSERT INTO pitcher_preview (
  game_date, pitcher_name, opposing_team
  )
WITH filtered AS (
  SELECT
    (raw_json->>'game_date')::date AS game_date,
    raw_json->>'preview_text' AS text
  FROM json_mlb_previews
  WHERE raw_json->>'preview_text' ILIKE '%- - - - -%'
),
matchups AS (
  SELECT
    f.game_date,
    match[1] AS opposing_team,
    match[2] AS pitcher_name
  FROM filtered f,
  regexp_matches(
    f.text,
    '([A-Z]{1,3})\nvs\.\n([A-Za-z.''\- ]+)',
    'g'
  ) AS match
)
SELECT
  game_date,
  pitcher_name,
  opposing_team
FROM matchups
ORDER BY game_date, opposing_team
ON CONFLICT (game_date, pitcher_name) DO NOTHING;




-- Batter preview inserts
INSERT INTO batter_preview (
  game_date, batter_name, hr, rbi, ab, avg, ops
)
WITH lines AS (
  SELECT
    raw_json,
    unnest(string_to_array(raw_json->>'preview_text', E'\n')) AS line,
    generate_series(1, cardinality(string_to_array(raw_json->>'preview_text', E'\n'))) AS line_number
  FROM json_mlb_previews
),
player_lines AS (
  SELECT 
    raw_json,
    line,
    regexp_matches(
      line,
      '(.+?)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\.\d{3})\s+(\.\d{3})'
    ) AS parts
  FROM lines
  WHERE line ~ '\d+\s+\d+\s+\d+\s+\.\d{3}\s+\.\d{3}'
)
SELECT
  (raw_json->>'game_date')::DATE AS game_date,
  regexp_replace(parts[1], '\s*(1B|2B|3B|SS|RF|LF|CF|C|DH|P|SP|RP)$', '') AS player_name,
  parts[2]::INT AS HR,
  parts[3]::INT AS RBI,
  parts[4]::INT AS AB,
  parts[5]::NUMERIC(5,3) AS AVG,
  parts[6]::NUMERIC(5,3) AS OPS
FROM player_lines
ON CONFLICT (game_date, batter_name) DO NOTHING;






COMMIT;
