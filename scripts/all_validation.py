from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from google.cloud import bigquery

# making schema in structtype format
class valid:
    pat1 = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
    pat2 = '[@_!#$%^&*()<>?/\|}{~:]'
    def __init__(self, spark, schema, inputfile, strschema, nullval=None, emailcol=None, spch=None,stchema = None):
        """

        :param spark:spark session object
        :param schema: schemafile for dataframe
        :param inputfile: input bucket location
        :param strschema: stringschema for removing integer
        :param nullval: null value tuple subset
        :param emailcol: email column subset
        :param spch: special character subset
        """
        self.spark = spark
        self.schema = schema
        self.inpfile = inputfile
        self.strsch = strschema
        self.nulval = nullval
        self.spch = spch
        self.emailcol = emailcol
        self.stschema = stchema
    def sch_a(self):
        with open(self.schema, 'r') as f:
            file = f.read()
            sch = json.loads(file)
            stsch = StructType.fromJson(sch)

        return stsch
#creating schema for bigquery table

    def get_field_schema(self,field):
        name = field['name']
        field_type = field.get('type', 'STRING')
        mode = field.get('mode', 'NULLABLE')
        fields = field.get('fields', [])

        if fields:
            subschema = []
            for f in fields:
                fields_res = self.get_field_schema(f)
                subschema.append(fields_res)
        else:
            subschema = []

        field_schema = bigquery.SchemaField(name=name,
                                            field_type=field_type,
                                            mode=mode,
                                            fields=subschema
                                            )
        return field_schema
    def sch_b(self):
        schema =  []
        with open(self.schema,'r') as f:
            file  = f.read()
            bigsch = json.loads(file)
        for field in bigsch:
            schema.append(self.get_field_schema(field))
        return schema

#null validation
    def nullval(self,schema):
        dfval = self.spark.read.format('csv').schema(schema).load(self.inpfile)
        nonull = dfval.dropna(subset = self.nulval)
        null = dfval.subtract(nonull)
        return nonull,null

#special character validation
    def spch_(self,nonenull,null):
        n = nonenull
        nn = null
        for column in self.spch:
            nospch= n.filter(~col(column).rlike(self.pat2))
        spch = n.subtract(nospch)
        spch_ = nn.union(spch)
        return nospch,spch_

  #email validation
    def email(self,nospch,spch):
        a = nospch
        b = spch

        for column in self.emailcol:
            emaildf = a.filter(~col(column).rlike(self.pat1))
        ef = a.subtract(emaildf)
        noef = b.union(emaildf)
        return ef,noef

  # first making dictionary of key value pairs of Schema

    #     with open(self.schema) as file:
    #         ds = file.read()
    #     dict_ = json.loads(ds)
    #
    # #########another method for structype conversion
    #
    #     mapping = {"string": StringType(), "integer": IntegerType(),
    #        "float": FloatType(), "date": DateType(),
    #        "long": LongType(), "double": DoubleType(),
    #        "timestamp": TimestampType(), "decimal": DecimalType()}
    #     sc = []
    #     for item in dict_:
    #         sc.append(StructField(item['name'], mapping.get(item['type'].lower()), item['nullable']))
    #     print(sc)
    #     schema = StructType(sc)
    #     return schema
