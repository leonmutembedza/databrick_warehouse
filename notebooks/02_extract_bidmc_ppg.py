# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_extract_bidmc_channel
# COMMAND ----------

# PPG extraction branch: run concurrently with the BIDMC ECG and respiration branches.
run_bidmc_channel("ppg", ["pleth", "ppg"])
