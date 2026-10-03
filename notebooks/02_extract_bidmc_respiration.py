# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_extract_bidmc_channel
# COMMAND ----------

# Respiration extraction branch: run concurrently with the BIDMC ECG and PPG branches.
run_bidmc_channel("respiration", ["resp", "respiration"])
