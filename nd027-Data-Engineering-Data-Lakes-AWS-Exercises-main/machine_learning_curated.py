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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1744158184175 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1744158184175")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1744158233302 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1744158233302")

# Script generated for node join and filter
SqlQuery6792 = '''
select distinct stt.*, at.x, at.y, at.z
from stt
join at on stt.sensorReadingTime = at.timestamp
'''
joinandfilter_node1744158267278 = sparkSqlQuery(glueContext, query = SqlQuery6792, mapping = {"stt":step_trainer_trusted_node1744158184175, "at":accelerometer_trusted_node1744158233302}, transformation_ctx = "joinandfilter_node1744158267278")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=joinandfilter_node1744158267278, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744158077799", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1744158388765 = glueContext.getSink(path="s3://cmulli/machine_learnin/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1744158388765")
machine_learning_curated_node1744158388765.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1744158388765.setFormat("json")
machine_learning_curated_node1744158388765.writeFrame(joinandfilter_node1744158267278)
job.commit()