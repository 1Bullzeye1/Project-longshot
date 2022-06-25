from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# #create spark session
spark=SparkSession\
    .builder\
    .appName("project_file")\
    .getOrCreate()

# #Custom Schema
#
customSchema= StructType([
                          StructField("dept_id",IntegerType()),\
                          StructField("dept_name",StringType()),\
                          StructField("location",StringType()),\
                          StructField("Currupt_record",StringType())\
                         ])

df= spark.read.format("com.databricks.spark.csv") \
    .option("mode","PERMISSIVE")\
    .option("badRecordsPath",r"Utils/")\
    .option("columnNameofCorruptRecord","Currupt_record")\
    .option("header","true")\
    .option("delimiter",",")\
    .format("csv") \
    .load(r"C:\Users\Administrator\Desktop\corrupt_files.csv",schema=customSchema)
df.show(truncate = False)
# InfoDF = spark.read\
#       .schema("dept_id Integer,dept_name String,location String")\
#       .option("mode","PERMISSIVE")\
#       .option("header",True)\
#       .csv(r"C:\corrupt_files.csv",schema=customSchema)
# InfoDF.show()