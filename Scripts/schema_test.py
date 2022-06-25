from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from collections import *
import json

spark = SparkSession.builder.appName("PCM").getOrCreate()

class Schema():

    with open(r"../config/schema.json") as file:
        ds = file.read()
    length = len(ds.split(','))

    items = []
    # print( length )

    for i in range(length):
        items.append((json.JSONDecoder(object_pairs_hook = OrderedDict).decode(ds)[i].items()))
    print(items)

    mapping = {"string": StringType, "integer": IntegerType,
               "float": FloatType, "date": DateType,
               "long": LongType, "double": DoubleType,
               "timestamp": TimestampType, "decimal": DecimalType  }
    # print(mapping)

S = Schema()
mapping = S.mapping
od1 = S.items


schema = []

for item in od1:
    schema.append([StructField(k,mapping.get(v.lower())(),True) for k,v in item])
schema = [y for x in [schema[i] for i in range(len(schema))] for y in x]

valschema = StructType(schema)
print(valschema)



df = spark.read.csv(r"C:\Users\Administrator\Desktop\spchar.csv",header = True,inferSchema = True)
print(df.schema)

###---------for making valid Schema-------------
dS = []
for i in df.schema:
    for j in valschema:
        if i==j:
            dS.append(i)
dataSchema = StructType(dS)
print(dataSchema)

