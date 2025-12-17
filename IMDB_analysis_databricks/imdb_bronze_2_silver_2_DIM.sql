-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE name_profession_silver
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
person_id,
primary_name,
profession
FROM LIVE.name_basics_bronze
LATERAL VIEW OUTER EXPLODE(SPLIT(primary_profession, ',')) AS profession;



CREATE OR REFRESH LIVE TABLE person_knownfor_title_silver
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
person_id,
primary_name,
known_for_title
FROM LIVE.name_basics_bronze
LATERAL VIEW OUTER EXPLODE(SPLIT(known_for_titles, ',')) AS known_for_title;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE title_basics_silver
TBLPROPERTIES ("quality" = "silver",   'delta.enableChangeDataFeed' = 'true')
AS
SELECT
  title_id,
  title_type,
  primary_title,
  original_title,
    CAST(is_adult AS INT) AS is_adult,
    CAST(start_year AS INT) AS year,
    CAST(runtime_minutes AS INT) AS runtime_minutes,
  genre,
  load_dt,
  source_file_path,
  source_file_name
FROM LIVE.title_basics_bronze
LATERAL VIEW OUTER EXPLODE(SPLIT(genres, ',')) AS genre
WHERE start_year IS NULL 
   OR start_year >= 1886;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE title_director_silver
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  title_id,
  director_id
FROM LIVE.title_crew_bronze
LATERAL VIEW OUTER EXPLODE(SPLIT(directors, ',')) AS director_id;


CREATE OR REFRESH LIVE TABLE title_writer_silver
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  title_id,
  writer_id
FROM LIVE.title_crew_bronze
LATERAL VIEW OUTER EXPLODE(SPLIT(writers, ',')) AS writer_id;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE title_episode_silver
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  title_id,
  parent_title_id,
    CAST(season_number AS INT) AS season_number,
    CAST(episode_number AS INT) AS episode_number
FROM LIVE.title_episode_bronze;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE title_rating_silver
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  title_id,
  average_rating,
  num_votes
FROM LIVE.title_ratings_bronze

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE title_akas_silver
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
    title_id,
    ordering,
    title,
    CASE 
        WHEN region IS NULL OR region = '\N' THEN NULL
        WHEN length(region) = 2 AND region RLIKE '^[a-zA-Z]{2}$'
            THEN upper(region)              
        ELSE NULL                          
    END AS region,
    CASE 
        WHEN language IS NULL OR language = '\N' THEN NULL
        WHEN length(language) = 2 AND language RLIKE '^[a-zA-Z]{2}$'
            THEN lower(language)           
        ELSE NULL                          
    END AS language,
    types,
    attributes,
    is_original_title
FROM LIVE.title_akas_bronze;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE title_principals_silver
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  title_id,
  ordering,
  person_id,
  lower(trim(NULLIF(category, '\N')))             AS category,
  trim(NULLIF(job, '\N'))                         AS job,
  trim(NULLIF(characters, '\N'))                  AS characters
FROM LIVE.title_principals_bronze;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_year
TBLPROPERTIES ("quality" = "gold")
AS
WITH years AS (
    SELECT sequence(year(current_date()) - 150, year(current_date())) AS year_array
),
exploded AS (
    SELECT explode(year_array) AS year_value
    FROM years
)
SELECT
    CAST(year_value AS INT)                  AS year_key,
    CAST(year_value AS INT)                  AS year,
    CAST(floor(year_value / 10) * 10 AS INT) AS decade
FROM exploded;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_genre
TBLPROPERTIES ("quality" = "gold")
AS
WITH distinct_genres AS (
    SELECT DISTINCT
        genre
    FROM LIVE.title_basics_silver
    WHERE genre IS NOT NULL
      AND genre <> '\N'
),
ranked AS (
    SELECT
        DENSE_RANK() OVER (ORDER BY genre) AS genre_key,
        genre                               AS genre_name
    FROM distinct_genres
)
SELECT
    genre_key,
    genre_name
FROM ranked;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_role
TBLPROPERTIES ("quality" = "gold")
AS
WITH principal_roles AS (
    SELECT DISTINCT
        lower(category) AS role_name,
        'job'           AS role_source
    FROM LIVE.title_principals_silver
    WHERE category IS NOT NULL
      AND category <> '\N'
),

profession_roles AS (
    SELECT DISTINCT
        lower(trim(prof)) AS role_name,
        'profession'      AS role_source
    FROM LIVE.name_profession_silver
    LATERAL VIEW explode(split(profession, ',')) AS prof
    WHERE prof IS NOT NULL
      AND trim(prof) <> ''
),

all_roles AS (
    SELECT role_name, role_source FROM principal_roles
    UNION ALL
    SELECT role_name, role_source FROM profession_roles
),

merged_roles AS (
    SELECT
        role_name,
        CASE
            WHEN array_sort(collect_set(role_source)) = array('job','profession')
                THEN 'both'
            WHEN array_sort(collect_set(role_source)) = array('job')
                THEN 'job'
            ELSE 'profession'
        END AS role_source
    FROM all_roles
    GROUP BY role_name
),
ranked AS (
    SELECT
        DENSE_RANK() OVER (ORDER BY role_name) AS role_key,
        role_name,
        role_source
    FROM merged_roles
)

SELECT
    role_key,
    role_name,
    role_source
FROM ranked;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_person
TBLPROPERTIES ("quality" = "gold")
AS
WITH prof_agg AS (
    SELECT
        person_id,
        max(primary_name)                               AS person_name,
        collect_set(lower(trim(profession)))           AS profession_set
    FROM LIVE.name_profession_silver
    WHERE profession IS NOT NULL
      AND trim(profession) <> ''
    GROUP BY person_id
),

knownfor_agg AS (
    SELECT
        person_id,
        collect_set(trim(known_for_title))             AS known_for_set
    FROM LIVE.person_knownfor_title_silver
    WHERE known_for_title IS NOT NULL
      AND trim(known_for_title) <> ''
    GROUP BY person_id
),

joined AS (
    SELECT
        p.person_id,
        p.person_name,
        concat_ws(',', profession_set)                 AS profession,
        concat_ws(',', k.known_for_set)                AS known_for_titles
    FROM prof_agg p
    LEFT JOIN knownfor_agg k
      ON p.person_id = k.person_id
)

SELECT
    DENSE_RANK() OVER (ORDER BY person_id)             AS person_key,
    person_id,
    person_name,
    profession,
    known_for_titles
FROM joined;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_language
TBLPROPERTIES ("quality" = "gold")
AS
WITH iso AS (
    SELECT
        lower(trim(code))       AS language_code,
        trim(language)          AS language_name
    FROM read_files(
        "/Volumes/workspace/imdb_project/datastore/language_code/",
        format => "csv",
        header => "true",
        inferSchema => "true"
    )
),
data_codes AS (
    SELECT DISTINCT
        lower(language) AS language_code
    FROM LIVE.title_akas_silver
    WHERE language IS NOT NULL
),
missing_in_iso AS (
    SELECT
        d.language_code,
        CAST(NULL AS STRING) AS language_name
    FROM data_codes d
    LEFT ANTI JOIN iso i
      ON d.language_code = i.language_code
),
final_union AS (
    SELECT * FROM iso
    UNION ALL
    SELECT * FROM missing_in_iso
)
SELECT
    language_code,
    language_name
FROM final_union;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_region
TBLPROPERTIES ("quality" = "gold")
AS
WITH iso AS (
    SELECT
        upper(trim(region_code))       AS region_code,
        trim(region_name)            AS region_name
    FROM read_files(
        "/Volumes/workspace/imdb_project/datastore/region/",
        format => "csv",
        header => "true",
        inferSchema => "true"
    )
),
data_codes AS (
    SELECT DISTINCT
        region AS region_code
    FROM LIVE.title_akas_silver
    WHERE region IS NOT NULL
),
missing_in_iso AS (
    SELECT
        d.region_code,
        CAST(NULL AS STRING) AS region_name
    FROM data_codes d
    LEFT ANTI JOIN iso i
      ON d.region_code = i.region_code
),
final_union AS (
    SELECT * FROM iso
    UNION ALL
    SELECT * FROM missing_in_iso
)
SELECT
    region_code,
    region_name
FROM final_union;