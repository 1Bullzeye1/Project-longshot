from pyspark.sql import SparkSession
##
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()
# .config("spark.jars",r"jars/spark-bigquery-with-dependencies_2.11-0.25.0.jar") \
###
print(spark)
# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
# bucket = "gs://temp_buck_bigquery"
# spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
tabledf = spark.read.format('bigquery') \
  .option('table', 'procrastinated-city-46778:bwt_student_dataset.routes') \
  .load()
tabledf.write.csv('gs://buk_zero/')

# tabledf.createOrReplaceTempView('words')


# Load data from another source

# df = spark.read.format("path_to_source")

# creating temp view

# Perform validation operation using sql or pyspark.
# airport.printSchema()
# valdf = df.fillna('(unknown)').na.fill(value=-1)

# Saving the data to BigQuery
# valdf.write.format('bigquery') \
# .option('table', 'dataset_name.valdf_output') \
# .save()
