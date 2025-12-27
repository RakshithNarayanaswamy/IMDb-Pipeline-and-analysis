🎬 IMDb Data Lakehouse & Analytics Platform

From Raw IMDb Datasets to Business-Ready Insights using Medallion Architecture

A comprehensive end-to-end data engineering and business intelligence project that transforms raw IMDb datasets into analytics-ready Gold marts, enabling deep insights into movies, genres, ratings, crew members, regions, and trends.

⸻

📌 Project Overview

This project implements a full-scale data lakehouse architecture using IMDb non-commercial datasets to support analytical and business intelligence use cases.
The solution follows the Medallion Architecture (Bronze → Silver → Gold) and applies data modeling, SCD-Type 2 dimensions, validation rules, and BI reporting to answer real-world movie analytics questions.

The platform enables stakeholders to analyze:
•	Movie performance by genre, year, rating, region
•	Directors, writers, and crew popularity
•	Adult vs non-adult content trends
•	Episode and season-level insights for non-movie titles

⸻

🎯 Business Objectives

As a Business Analyst / Data Consumer, this system enables the ability to:
•	Identify popular directors and writers across movies
•	Analyze top-rated movies by year and genre
•	Explore movie trends based on ratings and votes
•	Track adult vs non-adult movie distribution
•	Analyze movie runtime trends over time
•	Understand regional and language-based releases
•	Compare TV series seasons and episode counts vs ratings
•	Explore crew roles, jobs, and character involvement per title

⸻

🧱 Architecture Overview (Medallion Pattern)


flowchart TD
    A[IMDb Raw Files<br/>(TSV / GZ)] --> B[Bronze Layer<br/>Raw ingestion<br/>Schema enforcement<br/>Audit columns]
    B --> C[Silver Layer<br/>Cleansing & normalization<br/>Array explosion<br/>Data validation]
    C --> D[Gold Layer<br/>Star schema<br/>SCD Type 2 dimensions<br/>BI-ready marts]


⸻

📂 Data Sources

IMDb Non-Commercial Datasets:
•	title.basics – Movie & title metadata
•	title.akas – Regional & language titles
•	title.crew – Directors & writers
•	title.principals – Cast & crew roles
•	title.episode – Season & episode data
•	title.ratings – Ratings & vote counts
•	name.basics – Personnel & professions

Supporting reference data:
•	Country / Region codes
•	ISO-639 Language codes

⸻

🔄 Data Processing & Engineering

🟤 Bronze Layer (Raw Ingestion)
•	Ingested IMDb TSV files into Databricks Delta tables
•	Preserved raw structure with:
•	source_file
•	ingestion_timestamp
•	record_hash
•	Row count validation to ensure no data loss

⸻

⚪ Silver Layer (Cleansing & Standardization)
•	Removed invalid \N values
•	Exploded multi-valued arrays:
•	genres
•	directors
•	writers
•	primaryProfession
•	knownForTitles
•	Standardized:
•	Years, runtime, flags (adult/non-adult)
•	Region and language codes
•	Applied data quality checks before promoting records

⸻

🟡 Gold Layer (Analytics & BI)

Designed a star schema with surrogate keys and SCD-Type 2 dimensions.

Core Dimensions
•	dim_title (SCD-2)
•	dim_name (SCD-2)
•	dim_genre
•	dim_role
•	dim_language
•	dim_region
•	dim_year

Fact Tables
•	fact_movie_ratings
•	fact_title_crew
•	fact_episode_metrics

Key SCD-2 Features
•	version_number
•	is_current_flag
•	effective_start_date
•	effective_end_date

⸻

📊 Key Analytics & Insights

🎥 Movie & Rating Insights
	•	Top-rated movies by year and genre
	•	Rating distribution vs number of votes
	•	Runtime trends across decades

👥 Crew & Personnel Analysis
	•	Most popular directors and writers
	•	Personnel with multiple professions
	•	Crew role distribution per title

🌍 Regional & Language Trends
	•	Movie releases by country and region
	•	Language diversity per title
	•	Regional dominance in movie production

📺 Non-Movie Content Analysis
	•	Seasons vs episode counts
	•	Episode volume vs audience ratings
	•	Viewer preference trends for series content

⸻

📈 BI & Visualization Layer

Built interactive dashboards using Power BI / Tableau featuring:
	•	Star-schema-based modeling
	•	Optimized relationships
	•	DAX / calculated measures for:
	•	Average ratings
	•	Vote-weighted scores
	•	Year-over-year trends

Sample Dashboards:
	•	🎬 Movie Performance Dashboard
	•	👤 Director & Writer Popularity
	•	🌍 Regional Release Analysis
	•	📺 TV Series Episode Metrics

⸻

🛠️ Technologies Used
	•	Databricks – Data engineering & Delta Live Tables
	•	PySpark & SQL – Transformations and modeling
	•	Delta Lake – ACID-compliant storage
	•	Power BI / Tableau – BI & analytics
	•	ER Studio / Navicat – Data modeling
	•	GitHub – Version control & collaboration

💡 Business Value
	•	Converts raw, complex IMDb data into decision-ready insights
	•	Demonstrates enterprise-grade data engineering practices
	•	Enables scalable analytics for media, streaming, and entertainment use cases
	•	Strong portfolio project for Data Engineer / Analytics Engineer roles

⸻

🚀 Future Enhancements
	•	Incremental ingestion & CDC
	•	Streaming ingestion for near-real-time analytics
	•	ML models for rating prediction
	•	Genre-based recommendation engine
