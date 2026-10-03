# Databricks notebook source
# PPG extraction branch: run concurrently with the BIDMC ECG and respiration branches.
# MAGIC %run ./_extract_bidmc_channel

run_bidmc_channel("ppg", ["pleth", "ppg"])
