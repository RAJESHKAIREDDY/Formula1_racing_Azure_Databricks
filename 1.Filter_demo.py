# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filtered_df=races_df.filter("race_year= 2019")

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## USE ROUND to get required number of rows

# COMMAND ----------

races_filtered2_df=races_filtered_df.filter("race_year= 2019 and round<=5")

# COMMAND ----------

display(races_filtered2_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### another way of using filter condition

# COMMAND ----------

races_filtered3_df=races_filtered_df.filter((races_df["race_year"]==2019) & (races_df["round"]<=5))

# COMMAND ----------

display(races_filtered3_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## where is synonymous to Filter

# COMMAND ----------

races_filtered4_df=races_filtered_df.where(races_df["race_year"]==2019)

# COMMAND ----------

display(races_filtered4_df)

# COMMAND ----------


