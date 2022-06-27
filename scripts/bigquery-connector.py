from pyspark.sql import SparkSession
import sys
from google.cloud import storage,bigquery
from google.cloud import dataproc_v1 as dataproc




class sparksession:
    spark = SparkSession \
            .builder \
            .config("spark.jars", r"../Utils/gcs-connector-latest-hadoop2.jar") \
            .master('local[*]') \
            .appName('spark-bigquery-demo') \
            .getOrCreate()

    spark._jsc.hadoopConfiguration()\
            .set("google.cloud.auth.service.account.json.keyfile",
            r"C:\Users\Administrator\Downloads\procrastinated-city-46778-0957e750ee24.json")
    # spark._jsc.hadoopConfiguration()\
    #     .set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    # spark._jsc.hadoopConfiguration()\
    #     .set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    def bucketrdr(self,path):
        df = self.spark.read.option("header", True).csv(path=path)

    # .option("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    # .option("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
        return df.show()

if __name__ == "__main__":
    path = "gs://buk_zero/departments.csv"
    s = sparksession()
    s.bucketrdr(path)
