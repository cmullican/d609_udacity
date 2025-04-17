import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_raw
customer_raw_node1744770733197 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_raw", transformation_ctx="customer_raw_node1744770733197")

# Script generated for node filter
SqlQuery9880 = '''
with rn as (
    select row_number() over (partition by email 
                              order by serialnumber) rnum,
           cr.*
    from cr
)
select *
from rn
where rnum = 1;
'''
filter_node1744770754054 = sparkSqlQuery(glueContext, query = SqlQuery9880, mapping = {"cr":customer_raw_node1744770733197}, transformation_ctx = "filter_node1744770754054")

# Script generated for node customer_landing
customer_landing_node1744771124251 = glueContext.write_dynamic_frame.from_catalog(frame=filter_node1744770754054, database="stedi", table_name="customer_landing", transformation_ctx="customer_landing_node1744771124251")

job.commit()