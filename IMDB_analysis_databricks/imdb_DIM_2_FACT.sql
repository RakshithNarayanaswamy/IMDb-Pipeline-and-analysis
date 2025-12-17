-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE fact_title_credit
TBLPROPERTIES ("quality" = "gold")
AS
WITH principals AS (
    SELECT
        title_id,
        person_id,
        ordering,
        category,
        job,
        characters
    FROM LIVE.title_principals_silver
    WHERE title_id IS NOT NULL
      AND person_id  IS NOT NULL
),
with_title AS (
    SELECT
        p.*,
        dt.title_key,
        dt.year
    FROM principals p
    LEFT JOIN LIVE.dim_title dt
        ON p.title_id = dt.title_id
    WHERE dt.is_current_flag = 1
),
with_person AS (
    SELECT
        wt.*,
        dp.person_key
    FROM with_title wt
    LEFT JOIN LIVE.dim_person dp
        ON wt.person_id = dp.person_id
),
with_role_name AS (
    SELECT
        *,
        category AS role_name_norm
    FROM with_person
),
with_role AS (
    SELECT
        wr.*,
        dr.role_key
    FROM with_role_name wr
    LEFT JOIN LIVE.dim_role dr
        ON wr.role_name_norm = dr.role_name
),
with_year AS (
    SELECT
        wr.*,
        dy.year_key
    FROM with_role wr
    LEFT JOIN LIVE.dim_year dy
        ON CAST(wr.year AS INT) = dy.year
),
final AS (
    SELECT
        DENSE_RANK() OVER (
            ORDER BY
                title_key,
                person_key,
                role_key,
                year_key
        )                                              AS title_credit_id,
        title_key,
        person_key,
        role_key,
        year_key,
        ordering as credit_order,
        characters
    FROM with_year
    WHERE title_key IS NOT NULL
      AND person_key IS NOT NULL
      AND role_key   IS NOT NULL
      AND year_key   IS NOT NULL
)
SELECT *
FROM final;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_title_rating
TBLPROPERTIES ("quality" = "gold")
AS
WITH rating_src AS (
    SELECT
        title_id,
        average_rating,
        num_votes
    FROM LIVE.title_rating_silver        
    WHERE title_id IS NOT NULL
),


with_title AS (
    SELECT
        r.*,
        dt.title_key,
        dt.year
    FROM rating_src r
    LEFT JOIN LIVE.dim_title dt
        ON r.title_id = dt.title_id
    WHERE dt.is_current_flag = 1
),


with_year AS (
    SELECT
        wt.*,
        dy.year_key
    FROM with_title wt
    LEFT JOIN LIVE.dim_year dy
        ON CAST(wt.year AS INT) = dy.year
),

final AS (
    SELECT
        DENSE_RANK() OVER (ORDER BY title_key) AS rating_id,
        title_key,
        year_key,
        average_rating,
        num_votes
    FROM with_year
    WHERE title_key IS NOT NULL
      AND year_key  IS NOT NULL
)
SELECT *
FROM final;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE br_title_genre
TBLPROPERTIES ("quality" = "gold")
AS
WITH title_genre_raw AS (
    SELECT
        b.title_id,
        TRIM(genre_name) AS genre_name
    FROM LIVE.title_basics_silver b
    LATERAL VIEW OUTER EXPLODE(SPLIT(b.genre, ',')) AS genre_name
),

joined AS (
    SELECT DISTINCT
        dt.title_key,
        dg.genre_key
    FROM title_genre_raw r
    JOIN LIVE.dim_title dt
      ON r.title_id = dt.title_id
     AND dt.is_current_flag = 1
    JOIN LIVE.dim_genre dg
      ON LOWER(TRIM(r.genre_name)) = LOWER(TRIM(dg.genre_name)) 
),

dedup AS (
    SELECT DISTINCT
        title_key,
        genre_key
    FROM joined
),

ranked AS (
    SELECT
        DENSE_RANK() OVER (ORDER BY title_key, genre_key) AS title_genre_key,
        title_key,
        genre_key
    FROM dedup
)

SELECT
    title_genre_key,
    title_key,
    genre_key
FROM ranked;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE br_title_region_language
TBLPROPERTIES ("quality" = "gold")
AS
WITH akas_clean AS (
    SELECT DISTINCT
        title_id,
        region,
        language
    FROM LIVE.title_akas_silver
),

joined AS (
    SELECT
        dt.title_key,
        dr.region_code,
        dl.language_code
    FROM akas_clean a
    JOIN LIVE.dim_title dt
      ON a.title_id = dt.title_id
     AND dt.is_current_flag = 1
    JOIN LIVE.dim_region   dr
      ON a.region   = dr.region_code
    JOIN LIVE.dim_language dl
      ON a.language = dl.language_code
),

dedup AS (
    SELECT DISTINCT
        title_key,
        region_code,
        language_code
    FROM joined
),

ranked AS (
    SELECT
        DENSE_RANK() OVER (
            ORDER BY title_key, region_code, language_code
        ) AS title_region_language_key,
        title_key,
        region_code,
        language_code
    FROM dedup
)

SELECT
    title_region_language_key,
    title_key,
    region_code,
    language_code
FROM ranked;