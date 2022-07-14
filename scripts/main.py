import configparser
import re
from pyspark.sql import SparkSession
from all_validation import *
from google.cloud import storage,bigquery
from google.cloud import dataproc_v1 as dataproc

# ---------reading from config file-------------------
config = configparser.ConfigParser()
config.read(r"../config/config.ini")

inputfile = config.get("paths","inpfile")
schemafile = config.get("paths","jsonschema")
schemafile1 = config.get("paths","jsonschema1")
inpbuck = config.get("paths","inpbuckloc")
outvalbuck = config.get("paths","outvalidbuckloc")
outinvalbuck = config.get("paths","outcorruptbuckloc")
temp_buck = config.get("paths","temp_buck")
buck_nm = config.get("paths","buck_nm")
strschema = config.get("paths","strschema")
strschema1 = config.get("paths","strschema1")
key = config.get("paths","jsonkey1")

#validation inputs
nullval = config.get("columnval","null").replace(' ','').split(',')
spchval = config.get("columnval","spch").replace(' ','').split(',')
emailval = config.get("columnval","email").replace(' ','').split(',')

#bucket inputs and outputs
timestamp = config.get("bigquery","datetime")
project_id = config.get("bigquery","project_id")
dataset = config.get("bigquery","dataset")

class sparksession:
    spark = SparkSession.builder \
    .config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.5,com.google.guava:guava:r05') \
    .master('local[*]').appName('spark-bigquery-demo').getOrCreate()

    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", key)
    spark.conf.set('temporaryGcsBucket', temp_buck)


class filesingest:

    def fllist(self,bucket):
        #creating Client object
        client = storage.Client()
        buckets = client.get_bucket(bucket)
        files = list(buckets.list_blobs())
        for file in files:
            list1 = file.name.split('.')[0]
        return list1

    def upfile(self,dataframe,filename,location):

        filepath = '{}/{}.csv'.format(location,filename)

        dataframe.write.mode('overwrite').csv(filepath)

    def upinvalfile(self,dataframe,filename,location):
        filepath = '{}/{}.csv'.format(location, filename)

        dataframe.write.mode('overwrite').csv(filepath)

    def bigquery(self,project,dataset,filename,schema):

        client = bigquery.Client()
        table_id = f"{project}.{dataset}.{filename}"
        table = bigquery.Table(table_id, schema = schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

if __name__ == "__main__":
    s = sparksession()
    f = filesingest()
    filename = f.fllist(bucket = buck_nm)

    #creating dataframe object
    v = valid(spark = s.spark,schema = schemafile1,schema1 = schemafile,inputfile = inputfile,
              strschema =strschema1,nullval = nullval,spch = spchval,emailcol = emailval)

    #creating Schema
    struct = v.sch_a()
    print(struct)
    #creating bigquery schema
    bqschema = v.sch_b()
    print(bqschema)

    #getting nonenull and null dataframes
    nonnull,null = v.nullval(schema = struct)

    #getting dataframe from special character filter
    dfnospch,dfspch = v.spch_(nonenull = nonnull,null = null)

    #passing filtered dataframe to email validation

    fildf, nofildf = v.email(nospch = dfnospch,spch = dfspch)

    #uploading file to bucket:

    up = f.upfile(dataframe = fildf,location = outvalbuck,filename =  filename)
    upinval= f.upinvalfile(dataframe = nofildf,location = outinvalbuck,filename = filename)

    #creating table and storing data inside that

    # BQ_ = f.bigquery(project = project_id,dataset=dataset,filename = filename,schema = bqschema)


    fildf.write.format('bigquery') \
             .option('table','{}:{}.{}'.format(project_id,dataset,filename)) \
             .mode('append').save()

