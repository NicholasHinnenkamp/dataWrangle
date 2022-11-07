# Databricks notebook source
que_df = df = spark.read.parquet("/Workspace/Repos/nicholas.hinnenkamp@ndus.edu/dataWrangle/CleanedData/part-00000-tid-5840518577142024584-4d677880-34a7-4111-b564-06d5e70e2f48-145-1-c000.snappy.parquet")
display(que_df)
