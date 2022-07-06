from google.cloud import storage
from google.cloud import bigquery

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import gcsfs
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == '__main__':
    sparkconf = SparkConf().setAppName("Spark Context Init").setMaster("local[*]")
    sc = SparkContext(conf=sparkconf)
    # print(sc)
    spark = SparkSession.builder.appName("Spark Session Init").master("local[*]").getOrCreate()
    # print(spark)



obj_client = storage.Client()
obj_client1 = bigquery.Client()

print("this is bucket list :")
buck_list = obj_client.list_buckets()
for i in buck_list:
    print("  >  ",i.name)

buck_name = str(input("enter bucket name which you want to process: "))

buck = obj_client.get_bucket(buck_name)
filename = list(buck.list_blobs(prefix=""))
print("These are files in {} bucket".format(buck_name))
for i in filename:
    print("  >  ",i.name)

to_be_process = str(input("enter file name which you want to process: "))
read_file_path = "gs://{}/{}".format(buck_name, to_be_process)
main_df = spark.read.csv(read_file_path, header=True, inferSchema=True)

main_df_count = main_df.count()
null_df_count = main_df.dropna().count()

initial_schar_count = 0
for i in main_df.columns:
    special_char = main_df.filter(main_df[i].rlike('[@_!#$%^&*()<>?/\|}{~:]')).count()
    initial_schar_count = special_char + initial_schar_count

fs = gcsfs.GCSFileSystem("warm-alliance-352004")
with fs.open("gs://data_engineer1/stud_info.config") as stud:
    stud_info = json.load(stud)

d_types = {
    "string": StringType(),
    "integer": IntegerType()
}

schema_to_compare = StructType()

for i in stud_info:
    schema_to_compare.add(i["col_name"], d_types[i["col_type"]], True)

source_bucket = obj_client.bucket(buck_name)
source_file = source_bucket.blob(to_be_process)
pass_destination_bucket = obj_client.bucket("pass_file_pract1")
fail_destination_bucket = obj_client.bucket("fail_file_pract1")

schema11 = [
    bigquery.SchemaField("id", "integer"),
    bigquery.SchemaField("name", "string"),
    bigquery.SchemaField("class", "string"),
    bigquery.SchemaField("marks", "integer"),
    bigquery.SchemaField("gender", "string")
]

if main_df_count == null_df_count:
    if initial_schar_count < 1:
        if schema_to_compare == main_df.schema:
            file_copy = source_bucket.copy_blob(source_file, pass_destination_bucket, to_be_process)
            print("The {} file has successfully pass the validation criteria ".format(to_be_process))
            if 1 == 1:
                buck11 = obj_client.get_bucket(pass_destination_bucket)
                filename11 = list(buck11.list_blobs(prefix=""))
                print("These are files in {} bucket".format(pass_destination_bucket))
                for i in filename11:
                    print("  >  ",i.name)

                transfer_to_bq = str(input("enter file name which you want to transfer: "))
                ext_of_file = str(input("enter file ext which you want to transfer: "))

                table1 = bigquery.Table("warm-alliance-352004.cloudproject1.{}".format(transfer_to_bq), schema11)
                table = obj_client1.create_table(table1)

                load_conf = bigquery.LoadJobConfig(schema=schema11, skip_leading_rows=1)
                gcs_path = "gs://pass_file_pract1/{}{}".format(transfer_to_bq, ext_of_file)
                load_job = obj_client1.load_table_from_uri(
                    project="warm-alliance-352004",
                    source_uris=gcs_path,
                    destination="cloudproject1.{}".format(transfer_to_bq),
                    location="US",
                    job_config=load_conf,
                    job_id_prefix="loadcsv"
                )
                load_job.result()
                print("{} file has been transfer to BigQuery".format(to_be_process))

        else:
            file_copy = source_bucket.copy_blob(source_file, fail_destination_bucket, to_be_process)
            print("Opps, failed due to schema ")
    else:
        file_copy = source_bucket.copy_blob(source_file, fail_destination_bucket, to_be_process)
        print("Opps, failed due to Special char ")
else:
    file_copy = source_bucket.copy_blob(source_file, fail_destination_bucket, to_be_process)
    print("Opps, failed due to null value ")



