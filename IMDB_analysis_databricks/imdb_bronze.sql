-- Databricks notebook source
CREATE OR REPLACE STREAMING TABLE name_basics_bronze
TBLPROPERTIES(
  "delta.columnMapping.mode" = "name",
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,
  current_timestamp()                 AS load_dt,
  _metadata.file_path                 AS source_file_path,
  _metadata.file_name                 AS source_file_name
FROM STREAM cloud_files(
  '/Volumes/workspace/imdb_project/datastore/name_basics',
  'csv'
);


-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE title_basics_bronze
TBLPROPERTIES (
 
  "delta.columnMapping.mode" = "name",
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,

 
  current_timestamp()                 AS load_dt,
  _metadata.file_path                 AS source_file_path,
  _metadata.file_name                 AS source_file_name
  
FROM STREAM cloud_files(
  '/Volumes/workspace/imdb_project/datastore/title_basics',
  'csv'
);


-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE title_crew_bronze
TBLPROPERTIES (
 
  "delta.columnMapping.mode" = "name",
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,


  current_timestamp()                 AS load_dt,
  _metadata.file_path                 AS source_file_path,
  _metadata.file_name                 AS source_file_name

FROM STREAM cloud_files(
  '/Volumes/workspace/imdb_project/datastore/title_crew',
  'csv'
);


-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE title_episode_bronze
TBLPROPERTIES (

  "delta.columnMapping.mode" = "name",
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,

  current_timestamp()                 AS load_dt,
  _metadata.file_path                 AS source_file_path,
  _metadata.file_name                 AS source_file_name
  
      
FROM STREAM cloud_files(
  '/Volumes/workspace/imdb_project/datastore/title_episode',
  'csv'
);


-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE title_ratings_bronze
TBLPROPERTIES (
  
  "delta.columnMapping.mode" = "name",
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,

  current_timestamp()                 AS load_dt,
  _metadata.file_path                 AS source_file_path,
  _metadata.file_name                 AS source_file_name
  
FROM STREAM cloud_files(
  '/Volumes/workspace/imdb_project/datastore/title_ratings',
  'csv'
);


-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE title_akas_bronze
TBLPROPERTIES (
  
  "delta.columnMapping.mode" = "name",
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,

 
  current_timestamp()                 AS load_dt,
  _metadata.file_path                 AS source_file_path,
  _metadata.file_name                 AS source_file_name
  
FROM STREAM cloud_files(
  '/Volumes/workspace/imdb_project/datastore/title_akas',
  'csv'
);


-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE title_principals_bronze
TBLPROPERTIES (
 
  "delta.columnMapping.mode" = "name",
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,


  current_timestamp()                 AS load_dt,
  _metadata.file_path                 AS source_file_path,
  _metadata.file_name                 AS source_file_name
  
FROM STREAM cloud_files(
  '/Volumes/workspace/imdb_project/datastore/title_principals',
  'csv'
);
