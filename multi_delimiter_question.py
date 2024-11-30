# Databricks notebook source
from pyspark.sql.functions import split

# COMMAND ----------

data = ["1,Platinum\t1990|New York",
        "2,Reward\t1995|New Jesry",
        "3,Travel\t2001|Calefornia"
        ]
df = spark.createDataFrame(data, "string")
df.show(truncate= False)  
display(df)      

# COMMAND ----------

split_col  = split(df['value'],',|\t|\|')

# COMMAND ----------

df = df.withColumn('card_id',split_col.getItem(0))\
       .withColumn('card_category',split_col.getItem(1))\
           .withColumn('Year',split_col.getItem(2))\
               .withColumn('City',split_col.getItem(3))
               

# COMMAND ----------

df.show()

# COMMAND ----------

df2 = df.select("card_id","card_category","Year","City")
df2.show()
df2.printSchema()


# COMMAND ----------

df3 = df.select(df.card_id.cast('int'),"card_category",df.Year.cast('int'),"City")
df3.show()
df3.printSchema()


# COMMAND ----------

schema = StructType([StructFiled("card_id",IntegerType(),True)


]



)
