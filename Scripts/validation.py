from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import sys

spark = SparkSession.builder.getOrCreate()
# df = spark.createDataFrame([(1, 2, 3, 4)], [' 1', '%2', ',3', '(4)'])

df = spark.read.csv(r"C:\Users\Administrator\Desktop\spchar.csv",header = True)


print("----------Original Data------------")
df.show()
print("-----------filtered Data------------")

# df = spark.read.json()

# df1 = df.toDF(*[re.sub('[^\w]', '', c) for c in df.columns])
# df1.show()
#
# df1 = df.toDF(*[re.sub("[^a-zA-Z0-9]","", x)for x in df])
# df1.show()
x = df

for i in x.columns:
    x = x.withColumn(i,regexp_replace(i,"[^a-zA-Z0-9]",""))
x.show()


# y = df
# print(y)

for c in df.columns:
    good_records = df.filter(df[c].rlike('^[a-z|A-Z|0-9]*$'))
good_records.show()

for c in df.columns:
    bad_records = df.filter(df[c].rlike('[@_!#$%^&*()<>?/\|}{~:]'))
bad_records.show()
## --------using udf --------------

# filterdf = y.filter(udf())
# filterdf.show()




##--------------------------
# total = 0
# for col in df.columns:
#     count = df.filter(df[col].rlike('[@_!#$%^&*()<>?/\|}{~:]')).count()
#     total += count
#
# print(total)
# if total >= 1:
#     print("More than 1 record corrupted...!")
# else:
#     df.show()
#     df.write.csv("/output/csv/")