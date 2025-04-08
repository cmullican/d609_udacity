import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer landing
accelerometerlanding_node1743987235151 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometerlanding_node1743987235151")

# Script generated for node customer_trusted
customer_trusted_node1743987315066 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1743987315066")

# Script generated for node filter
SqlQuery4939 = '''
select distinct ct.* from al
join ct on al.user = ct.email
'''
filter_node1743987255288 = sparkSqlQuery(glueContext, query = SqlQuery4939, mapping = {"al":accelerometerlanding_node1743987235151, "ct":customer_trusted_node1743987315066}, transformation_ctx = "filter_node1743987255288")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=filter_node1743987255288, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743981919602", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1743987480878 = glueContext.getSink(path="s3://cmulli/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1743987480878")
customercurated_node1743987480878.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
customercurated_node1743987480878.setFormat("json")
customercurated_node1743987480878.writeFrame(filter_node1743987255288)
job.commit()