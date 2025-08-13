# Databricks notebook source
print(spark)

# COMMAND ----------

 dbutils.fs.mkdirs("/foobar/")

# COMMAND ----------

 dbutils.fs.put("/foobar/baz.txt", "Hello, World!")


# COMMAND ----------

 dbutils.fs.head("/foobar/baz.txt")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/foobar"))

# COMMAND ----------

#Create DataFrame  Directly
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]
 
df = spark.createDataFrame(data=data2)
df.printSchema()
display(df)


# COMMAND ----------


data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

columns = ["first_name","middle_name","last_name","gender","salary"]
df = spark.createDataFrame(data=data2,schema = columns)
df.printSchema()
display(df)


# COMMAND ----------


df=spark.read.csv("/FileStore/tables/Orders1.csv", header=True, inferSchema=True)

df.show()


# COMMAND ----------

pandasdf=df.toPandas()
print(pandasdf)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC dbutils.fs.put("/tmp/test.json", """
# MAGIC {"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}}
# MAGIC {"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}}
# MAGIC {"string":"string3","int":3,"array":[3,6,9],"dict": {"key": "value3", "extra_key": "extra_value3"}}
# MAGIC """, true)
# MAGIC

# COMMAND ----------

df=spark.read.json("/tmp/test.json")
display(df)

# COMMAND ----------

print("hi")

# COMMAND ----------

data = [("James","Sales",34), ("Michael","Sales",56), \
    ("Robert","Sales",30), ("Maria","Finance",24) ]

columns= ["name","dept","age"]
dataframe1 = spark.createDataFrame(data = data, schema = columns)
dataframe1.printSchema()



# COMMAND ----------

#dataframe1.write.parquet('tmp/output/people3.parquet')
dataframe1.write.mode("append").partitionBy("age").parquet("dbfs:/tmp/output/first.parquet")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/output/people3.parquet"))

# COMMAND ----------

ad=spark.read.parquet("dbfs:/tmp/output/first.parquet/age=34")
ad.show()

# COMMAND ----------

data1 = [("James1","Sales1",34), ("Michael1","Sales",56), \
    ("Robert1","Sales1",30), ("Maria1","Finance",24) ]

columns1= ["name","dept","age"]
dataframe2 = spark.createDataFrame(data = data1, schema = columns1)

dataframe2.write.mode('append').parquet('tmp/output/first.parquet')


# COMMAND ----------

data2 = [(2012,8,"Batman",9.8),
           (2012,8,"Hero",8.7),
           (2012,7,"Robot",5.5),
           (2011,7,"git",2.0)
  ]

columns = ["year","month","title","rating"]
df = spark.createDataFrame(data=data2,schema = columns)

df.write.mode("overwrite").partitionBy("year","month").format("avro").save("/tmp/test_Dataset01")




# COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/test_Dataset01/"))

# COMMAND ----------

df= spark.read.format("avro").load("/tmp/test_Dataset01")
display(df)

# COMMAND ----------

data = [{
    'col1': 'Category A',
    'col2': 100
}, {
    'col1': 'Category B',
    'col2': 200
}, {
    'col1': 'Category C',
    'col2': 300
}]

df = spark.createDataFrame(data)
df.show()

# Save as Orc
df.write.format('orc').mode('overwrite').save('/FileStore/tables/userdata1_orc')




# COMMAND ----------

file_location = "/FileStore/tables/userdata1_orc"
file_type = "orc"

infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .load(file_location)

df.show()



# COMMAND ----------


