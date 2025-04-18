START TRANSACTION;

INSERT INTO player2 (player_id, player_name, team_abbreviation)
SELECT DISTINCT 
  row_data->>4 AS player_id,
  row_data->>5 AS player_name,
  row_data->>2 AS team_abbreviation
FROM nba_box_scores,
  LATERAL jsonb_path_query(
    raw_json,
    '$.resultSets[*] ? (@.name == "PlayerStats").rowSet[*]'
  ) AS row_data
WHERE row_data->>4 IS NOT NULL
ON CONFLICT (player_id) DO NOTHING;;


INSERT INTO stats2 (
  game_id, player_id, min, fga, fg_pct, oreb, reb, ast, stl, blk, tos, pts
)
SELECT
  row_data->>0 AS game_id,
  row_data->>4 AS player_id,
  make_interval(
   mins := COALESCE(FLOOR((split_part(row_data->>9, ':', 1))::float)::int, 0),
   secs := COALESCE(FLOOR((split_part(row_data->>9, ':', 2))::float)::int, 0)),
  (row_data->>11)::float,              
  (row_data->>12)::numeric,            
  (row_data->>18)::float,          
  (row_data->>20)::float,            
  (row_data->>21)::float,              
  (row_data->>22)::float,            
  (row_data->>23)::float,              
  (row_data->>24)::float,             
  (row_data->>26)::float               
FROM nba_box_scores,
  LATERAL jsonb_path_query(
    raw_json,
    '$.resultSets[*] ? (@.name == "PlayerStats").rowSet[*]'
  ) AS row_data
WHERE row_data->>4 IS NOT NULL
  AND row_data->>9 IS NOT NULL          
  AND row_data->>9 NOT LIKE 'DNP%'     
ON CONFLICT (game_id, player_id) DO NOTHING;



INSERT INTO games2 (game_id)
SELECT DISTINCT 
  row_data->>0 AS game_id
FROM nba_box_scores,
     LATERAL jsonb_path_query(
       raw_json,
       '$.resultSets[*] ? (@.name == "PlayerStats").rowSet[*]'
     ) AS row_data
WHERE row_data->>0 IS NOT NULL
ON CONFLICT (game_id) DO NOTHING;




COMMIT;
