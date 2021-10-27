# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1. Data Prep
# MAGIC 
# MAGIC 1. Upload the datasets to Databricks DBFS
# MAGIC 2. For each dataset, define its location, set CSV options, and create data frame
# MAGIC 3. Create tables in Snowflake for data to load into
# MAGIC 4. Connect to Snowflake using the Spark connector & load data

# COMMAND ----------

# Set static variables for notebook

# File type and folder path
file_type = "csv"
data_in_folder = "/FileStore/tables/"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# COMMAND ----------

### Dataset: airlines ###

# File location
airlines_file_path = data_in_folder + "airlines.csv"

# The applied options are for CSV files. For other file types, these will be ignored.
airlines_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(airlines_file_path)

display(airlines_df)

# COMMAND ----------

### Dataset: airports ###

# File location
airports_file_path = data_in_folder + "airports.csv"

# The applied options are for CSV files. For other file types, these will be ignored.
airports_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(airports_file_path)

display(airports_df)

# COMMAND ----------

### Dataset: flights ###

# File location
flights_file_path = data_in_folder + "partition*.csv"

# The applied options are for CSV files. For other file types, these will be ignored.
flights_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(flights_file_path)

display(flights_df)

# COMMAND ----------

### Get counts for validation in Snowflake ###

# Would also validate file -> dataframe but file path on blob/dbfs seems to be inaccessible to Python/Bash

# Airlines
# airlines_df.count()
# 14

# Airports
# airports_df.count()
# 322

# Airports
# flights_df.count()
# 3920766

# COMMAND ----------

# Define a function to print a table create statement from a dataframe (should be used only for a static dataset)

def df_to_sf_sql(df, tbl_name):
  # Create column type dict
  datatype_dict = dict(df.dtypes)
  df.registerTempTable("df_table")

  # For each column, determine data type and add line in the column creation string array
  column_create_strs = []
  for field in df.schema.fields:
    field_name = field.name
    field_type = datatype_dict[field_name]
    if field_type == 'string':
      # If the field is a string type, create a Spark SQL statement to get the max length of that field in the DF
      max_len_sql = f"SELECT LENGTH({field_name}) as maxval FROM df_table ORDER BY LENGTH({field_name}) DESC LIMIT 1"
      field_max_len = spark.sql(max_len_sql).first().asDict()['maxval']
      # Add the constructed column create statement
      column_create_strs.append(field_name + ' ' + f"VARCHAR({field_max_len})")
    elif field_type == 'int':
      # Assume bigint for any integer type
      column_create_strs.append(field_name + ' ' + f"BIGINT")
    elif field_type == 'double':
      column_create_strs.append(field_name + ' ' + f"DOUBLE")
    else:
      # If no other type was identifie, use variant
      column_create_strs.append(field_name + ' ' + f"VARIANT")

  # Print the table create statement
  print(f"CREATE OR REPLACE TABLE {tbl_name} (")
  print("\t" + "\r\n\t,".join(column_create_strs))
  print(");")

# COMMAND ----------

df_to_sf_sql(airlines_df, "AIRLINES")

# COMMAND ----------

df_to_sf_sql(airports_df, "AIRPORTS")

# COMMAND ----------

df_to_sf_sql(flights_df, "FLIGHTS")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Here - take table create statements and make tables in Snowflake, then continue

# COMMAND ----------

# Hardcode username and password for Snowflake - this would be done using secrets in production
user = "asteinbergs"
password = "EmbsxMf3cC#VE7"

# snowflake connection options
options = {
  "sfUrl": "od23954.us-east-2.aws.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "USER_ANDREW",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "INTERVIEW_WH"
}

# COMMAND ----------

# Write dataframes from Databricks into Snowflake tables which were just created

airlines_df.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRLINES") \
  .mode("append") \
  .save()

# COMMAND ----------

airports_df.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRPORTS") \
  .mode("append") \
  .save()

# COMMAND ----------

flights_df.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "FLIGHTS") \
  .mode("append") \
  .save()
