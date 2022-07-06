from pyspark.sql import SparkSession
import configparser
import sys
from google.cloud import storage,bigquery
from google.cloud import dataproc_v1 as dataproc

# ---------reading from config file-------------------
config = configparser.ConfigParser()
config.read(r"../config/config.ini")

inputfile = config.get("paths","inpfile")

class sparksession:
    spark = SparkSession \
            .builder \
            .config("spark.jars", r"../Utils/gcs-connector-latest-hadoop2.jar") \
            .master('local[*]') \
            .appName('spark-bigquery-demo') \
            .getOrCreate()

    spark._jsc.hadoopConfiguration()\
            .set("google.cloud.auth.service.account.json.keyfile",
            r"../config/key.json")
    spark._jsc.hadoopConfiguration()\
        .set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark._jsc.hadoopConfiguration()\
        .set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    def bucketrdr(self,path):
        df = self.spark.read.option("header", True).csv(path=path)
        return df

if __name__ == "__main__":
    s = sparksession()
    df = s.bucketrdr(inputfile)
    df.show()






