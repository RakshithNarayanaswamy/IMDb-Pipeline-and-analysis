-- Databricks notebook source
select * from workspace.imdb_project.name_basics_bronze

-- COMMAND ----------

select * from workspace.imdb_project.name_profession_silver

-- COMMAND ----------

select * from workspace.imdb_project.person_knownfor_title_silver

-- COMMAND ----------

select * from workspace.imdb_project.title_basics_bronze 

-- COMMAND ----------

select * from workspace.imdb_project.title_basics_silver where title_type = "movie"

-- COMMAND ----------

select * from workspace.imdb_project.title_crew_bronze

-- COMMAND ----------

select * from workspace.imdb_project.title_director_silver

-- COMMAND ----------

select * from workspace.imdb_project.title_writer_silver

-- COMMAND ----------

select * from workspace.imdb_project.title_episode_bronze

-- COMMAND ----------

select * from workspace.imdb_project.title_episode_silver

-- COMMAND ----------

select * from workspace.imdb_project.title_ratings_bronze

-- COMMAND ----------

select * from workspace.imdb_project.title_rating_silver

-- COMMAND ----------

select * from workspace.imdb_project.title_akas_bronze

-- COMMAND ----------

select * from workspace.imdb_project.title_akas_silver

-- COMMAND ----------

select * from workspace.imdb_project.title_principals_bronze

-- COMMAND ----------

select * from workspace.imdb_project.title_principals_silver where title_id ="tt18974146"

-- COMMAND ----------

SELECT DISTINCT
        person_id,
        trim(primary_name)                  AS person_name,
        lower(trim(profession))            AS primary_profession
    FROM workspace.imdb_project.name_profession_silver

-- COMMAND ----------

drop table if exists workspace.imdb_project.region_silver

-- COMMAND ----------

SELECT DISTINCT
    s.region AS missing_region_code
FROM workspace.imdb_project.title_akas_silver s
LEFT JOIN workspace.imdb_project.dim_region r
    ON s.region = r.region_code
WHERE s.region IS NOT NULL
  AND r.region_code IS NULL;

-- COMMAND ----------

SELECT DISTINCT
    s.language AS missing_language_code
FROM workspace.imdb_project.title_akas_silver s
LEFT JOIN workspace.imdb_project.dim_language l
    ON s.language = l.language_code
WHERE s.language IS NOT NULL
  AND l.language_code IS NULL;

-- COMMAND ----------

select * from workspace.imdb_project.dim_title where title_type = "movie"

-- COMMAND ----------

select * from workspace.imdb_project.dim_region

-- COMMAND ----------

select * from workspace.imdb_project.dim_language

-- COMMAND ----------

select * from workspace.imdb_project.dim_genre

-- COMMAND ----------

select * from workspace.imdb_project.dim_role

-- COMMAND ----------

select * from workspace.imdb_project.dim_year

-- COMMAND ----------

select * from workspace.imdb_project.fact_title_credit where title_key = "353068"

-- COMMAND ----------

select * from workspace.imdb_project.fact_title_rating

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.imdb_project.dim_title")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/imdb_project/dataextract/dim_title")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.imdb_project.dim_person")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/imdb_project/dataextract/dim_person")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.imdb_project.fact_title_rating")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/imdb_project/dataextract/fact_title_rating")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.imdb_project.fact_title_credit")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/imdb_project/dataextract/fact_title_credit")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.imdb_project.br_title_region_language")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/imdb_project/dataextract/br_title_region_language")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.imdb_project.br_title_genre")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/imdb_project/dataextract/br_title_genre")

-- COMMAND ----------

SELECT
  title_id,
  title_type,
  primary_title,
  original_title,
  TRY_CAST(is_adult AS INT) AS is_adult,
  TRY_CAST(start_year AS INT) AS year,
  TRY_CAST(runtime_minutes AS INT) AS runtime_minutes,
  genres,
  load_dt,
  source_file_path,
  source_file_name
FROM workspace.imdb_project.title_basics_bronze
LATERAL VIEW OUTER EXPLODE(SPLIT(genres, ',')) AS genre
WHERE start_year IS NULL 
   OR TRY_CAST(start_year AS INT) <= 1890;