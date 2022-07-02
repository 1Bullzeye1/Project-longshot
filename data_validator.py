from pyspark import SparkConf,  SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# sc = SparkContext(conf=SparkConf())
# spark = SparkSession(sc)
#
# text = ["March 8,2019", "march 8, 2019", "march 8 2019",
#         "mar 30 2019", "Countermarch 8,2019 feet", "marched"]
#
# df = spark.createDataFrame(text, StringType()).toDF("text")
#
# df = df.withColumn("date_from_text", F.regexp_extract(df.text, r"(\b(?:[M|m]ar(?:ch)?)\b [0-9]+,?(?: |)\d{4})", 0))
# df.show()

a=1
b=int('1')

print(a/2)
print(b/2)