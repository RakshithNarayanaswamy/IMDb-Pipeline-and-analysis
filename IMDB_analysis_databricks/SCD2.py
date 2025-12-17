# Databricks notebook source
import dlt
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    lead
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    lead,
    row_number,
    collect_set,
    sort_array,
    concat_ws
)
from pyspark.sql.window import Window

@dlt.table(name="dim_title_type2_stage")
def dim_title_type2_stage():
    basics_raw = spark.table("workspace.imdb_project.title_basics_silver") 

    basics = (
        basics_raw
        .groupBy(
            "title_id",
            "title_type",
            "primary_title",
            "original_title",
            "is_adult",
            "year",
            "runtime_minutes",
            "load_dt"
        )
        .agg(
            sort_array(collect_set(col("genre"))).alias("genre_array")
        )
        .withColumn("genres", concat_ws(",", col("genre_array")))
        .drop("genre_array")
    )

    episodes = spark.table("workspace.imdb_project.title_episode_silver")

    df = (
        basics.alias("b")
        .join(episodes.alias("e"), col("b.title_id") == col("e.title_id"), "left")
        .where(col("b.title_id").isNotNull())
        .select(
            col("b.title_id").alias("title_id"),
            col("e.season_number"),
            col("e.episode_number"),
            col("e.parent_title_id").alias("parent_title_id"),
            col("b.original_title"),
            col("b.primary_title"),
            col("b.title_type"),
            col("b.is_adult").cast("int").alias("is_adult"),
            col("b.year").alias("year"),
            col("b.runtime_minutes"),
            col("b.genres"),                    
            col("b.load_dt").alias("load_ts")
        )
    )

    df = df.withColumn("event_ts", col("load_ts").cast("timestamp"))

    w = Window.partitionBy("title_id").orderBy("event_ts")

    df = df.withColumn("effective_start_date", col("event_ts").cast("date"))

    df = df.withColumn("next_event_ts", lead("event_ts").over(w))
    df = df.withColumn("effective_end_date", col("next_event_ts").cast("date"))

    df = df.withColumn("is_current_flag", (col("next_event_ts").isNull()).cast("int"))

    df = df.withColumn("version_number", row_number().over(w))

    return df.select(
        "title_id",
        "season_number",
        "episode_number",
        "parent_title_id",
        "original_title",
        "primary_title",
        "title_type",
        "is_adult",
        "year",
        "runtime_minutes",
        "genres",
        "effective_start_date",
        "effective_end_date",
        "is_current_flag",
        "version_number",
        "load_ts"
    )

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window

@dlt.table(name="dim_title")
def dim_title():
    base = (
        dlt.read("dim_title_type2_stage")
        .filter(F.col("is_current_flag") == 1)
    )
    w_key = Window.orderBy("title_id")
    with_keys = base.withColumn(
        "title_key",
        F.dense_rank().over(w_key)
    )
    final_df = with_keys.select(
        "title_key",
        "season_number",
        "episode_number",
        "parent_title_id",    
        "title_id",
        "original_title",
        "primary_title",
        "title_type",
        "is_adult",
        "year",
        "runtime_minutes",
        "genres",
        "effective_start_date",
        "effective_end_date",
        "is_current_flag",
        "version_number",
        "load_ts"
    )

    return final_df