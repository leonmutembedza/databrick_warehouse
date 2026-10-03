# Databricks notebook source
# ECG extraction branch: run concurrently with the BIDMC PPG and respiration branches.
# MAGIC %run ./_extract_bidmc_channel

run_bidmc_channel("ecg", ["ii", "v", "ecg"])
