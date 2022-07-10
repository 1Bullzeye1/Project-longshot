from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# df = spark.read.json()

# df1 = df.toDF(*[re.sub('[^\w]', '', c) for c in df.columns])
# df1.show()
#
# df1 = df.toDF(*[re.sub("[^a-zA-Z0-9]","", x)for x in df])
# df1.show()

# for i in x.columns:
#     x = x.withColumn(i,regexp_replace(i,"[^a-zA-Z0-9]",""))
# x.show()


# y = df
# print(y)
pat1 = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
pat2 = '^[a-z|A-Z|0-9]*$'
#
#     if c != "emailid":
#         d1 = df.filter(col(c).rlike(pat2))
#         d1.show()

# for c in df.columns:
#     if c == "emailid":
#         pass
#     else:
#         d3 = df.filter((col(c).rlike(pat2)))
#         # d3.show()
# d3.show()
#
# dc = d3.filter(col(c).rlike(pat1))
# dc.show()
#
# print("------------------without for loop-----------")
# d2 = df.filter(col("id").rlike(pat2) & col("name").rlike(pat2) & col("city").rlike(pat2))
# d2.show()
# for c in d2.columns:
#
#     d1 = d2.filter((col(c).rlike(pat1)))
# d1.show()
#
# good_records = d1.join(d2,how = 'inner').distinct()
# good_records.show()
#
# for c in df.columns:
#     bad_records = df.filter(df[c].rlike('[@_!#$%^&*()<>?/\|}{~:]'))
# bad_records.show()
# # --------using udf --------------
#
# filterdf = bad_records.filter(udf())
# filterdf.show()
#
#
#
#
# ##--------------------------
# # total = 0
# # for col in df.columns:
# #     count = df.filter(df[col].rlike('[@_!#$%^&*()<>?/\|}{~:]')).count()
# #     total += count
# #
# # print(total)
# # if total >= 1:
# #     print("More than 1 record corrupted...!")
# # else:
# #     df.show()
#     df.write.csv("/output/csv/")