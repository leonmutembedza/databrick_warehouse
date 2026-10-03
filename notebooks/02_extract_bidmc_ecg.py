# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_extract_bidmc_channel
# COMMAND ----------

# ECG extraction branch: run concurrently with the BIDMC PPG and respiration branches.
run_bidmc_channel("ecg", ["ii", "v", "ecg"])
