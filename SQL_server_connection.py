# Databricks notebook source
host_name = "thecloudbox.database.windows.net"
database_name = "thecloudbox"
table_name = "SalesLT.Address"
port = "1433"

# COMMAND ----------

# MAGIC %md
# MAGIC store user_name = ""
# MAGIC password = "" in sepearte notebook copy the path 

# COMMAND ----------

# MAGIC %run /Users/anupgupta05@outlook.com/credentials

# COMMAND ----------

df = (spark.read
  .format("sqlserver")
  .option("host", host_name)
  .option("port", port) 
  .option("user", user_name)
  .option("password", password)
  .option("database", database_name)
  .option("dbtable", table_name)
  .load()
)


# COMMAND ----------

dbutils.widgets.text("table_name","SalesLT.Address")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

connection_properties = {
    "user": user_name,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# JDBC URL
jdbc_url = f"jdbc:sqlserver://{host_name}:{port};database={database_name}"

# Reading data from SQL Server
df_J = spark.read.jdbc(url=jdbc_url, table=f"{table_name}", properties=connection_properties)



# COMMAND ----------

display(df_J)

# COMMAND ----------

df_transformed = df_J.withColumnRenamed('Phone','Mobile_Number')

# COMMAND ----------



#display(df_transformed)
# Writing data back to SQL Server
df_transformed.write.jdbc(url=jdbc_url, table="SalesLT.Customer2", mode="append", properties=connection_properties)


# COMMAND ----------


