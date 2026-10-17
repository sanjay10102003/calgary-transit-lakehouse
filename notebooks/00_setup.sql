-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC Schemas for each layer and the Volume the poller uploads to. Free Edition only has the `workspace` catalog.

-- COMMAND ----------

USE CATALOG workspace;

CREATE SCHEMA IF NOT EXISTS transit_bronze COMMENT 'raw feeds and schedule';
CREATE SCHEMA IF NOT EXISTS transit_silver COMMENT 'cleaned data';
CREATE SCHEMA IF NOT EXISTS transit_gold   COMMENT 'reporting tables';

CREATE VOLUME IF NOT EXISTS transit_bronze.landing;

-- COMMAND ----------

-- should show one folder per feed once the poller has uploaded something
LIST '/Volumes/workspace/transit_bronze/landing/';
