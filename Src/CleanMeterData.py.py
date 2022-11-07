# Databricks notebook source
spark.conf.set(
 "fs.azure.account.key.datawrangle.dfs.core.windows.net",
 dbutils.secrets.get(scope="ElectricalUsageScope", key="electricalusage-key"))
uri = "abfss://blob@datawrangle.dfs.core.windows.net/"
results_df = spark.read.option("delimiter", "|").csv(uri+"input/DailyMeterData.dat", header=True)

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable

def melt_df(
        df: DataFrame,
        id_vars: Iterable[str], value_vars: Iterable[str],
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""
        
    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))
    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
            
    return _tmp.select(*cols)

# COMMAND ----------

QB_df = results_df.select('Meter Number', 'Customer Account Number', 'Serial Number', 'Port', 'Channel', 'Conversion Factor', 'Data Type', 'Start Date', 'Start Time', 'QC#1', 'QC#2', 'QC#3', 'QC#4', 'QC#5', 'QC#6', 'QC#7', 'QC#8', 'QC#9', 'QC#10', 'QC#11', 'QC#12', 'QC#13', 'QC#14', 'QC#15', 'QC#16', 'QC#17', 'QC#18', 'QC#19', 'QC#20', 'QC#21', 'QC#22', 'QC#23', 'QC#24')
for i in range(1,25):
    QB_df = num_df.withColumnRenamed('QC#' + str(i), str(i)) 
result_df = results_df.drop('QC#1', 'QC#2', 'QC#3', 'QC#4', 'QC#5', 'QC#6', 'QC#7', 'QC#8', 'QC#9', 'QC#10', 'QC#11', 'QC#12', 'QC#13', 'QC#14', 'QC#15', 'QC#16', 'QC#17', 'QC#18', 'QC#19', 'QC#20', 'QC#21', 'QC#22', 'QC#23', 'QC#24', '_c58')
num_df = result_df
for i in range(1,25):
    num_df = num_df.withColumnRenamed('Interval#' + str(i), str(i)) 
print(QC_df)

# COMMAND ----------

result_df = melt_df(num_df,   # Data frame
                    ['Meter Number', 'Customer Account Number', 'Serial Number', 'Port', 'Channel', 'Conversion Factor', 'Data Type', 'Start Date', 'Start Time'],     # Columns to keep
                    num_df.columns[10:34], # Columns to convert to long
                    'InevertalHours',            # Name of new column that used to be the column header.
                    'InvertalValue')# Name of new column containing the value.
second_df = melt_df(QC_df,   # Data frame
                    ['Serial Number'],     # Columns to keep
                    QC_df.columns[0:24], # Columns to convert to long
                    'InevertalHour',            # Name of new column that used to be the column header.
                    'QCCode')
new_df = result_df.join(second_df, how="left").where(result_df["Serial Number"] == second_df["Serial Number"]).where(result_df["InevertalHours"] == second_df["InevertalHour"]).drop("Serial Number", "InevertalHours")
display(new_df)

# COMMAND ----------

noreverse_df = new_df.filter(new_df["Data Type"] != "Reverse Energy in Wh").filter(new_df["Data Type"] != "Net Energy in WH")
display(noreverse_df)

# COMMAND ----------

three_df = noreverse_df.filter(noreverse_df["QCCode"] == 3)
display(three_df)

# COMMAND ----------

distinct_pf = three_df.distinct()
display(distinct_pf)

# COMMAND ----------

sort_df = distinct_pf.sort(['Customer Account Number','Meter Number', 'Data Type','Start Date', 'InevertalHour'], )
display(sort_df)

# COMMAND ----------

sort_df.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"output/CSV")
sort_df.coalesce(1).write.option('header',True).mode('overwrite').parquet(uri+"output/Parquet")

# COMMAND ----------

spark.read.csv(uri+"input/DailyMeterData.dat", header=True).write.option('header',True).mode('overwrite').parquet(uri+"output/CustomerData")
