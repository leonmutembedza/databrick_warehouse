# Databricks notebook source
# Respiration extraction branch: run concurrently with the BIDMC ECG and PPG branches.
# MAGIC %run ./_extract_bidmc_channel

run_bidmc_channel("respiration", ["resp", "respiration"])
