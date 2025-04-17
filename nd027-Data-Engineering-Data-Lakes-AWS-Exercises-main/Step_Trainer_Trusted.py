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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1744777062263 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1744777062263")

# Script generated for node Customer Trusted
CustomerTrusted_node1744775159654 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerTrusted_node1744775159654")

# Script generated for node filter step_trainer data
SqlQuery10774 = '''
select stl.*
from stl 
join cc on stl.serialNumber = cc.serialNumber
where stl.sensorreadingtime > 0
'''
filterstep_trainerdata_node1744087799450 = sparkSqlQuery(glueContext, query = SqlQuery10774, mapping = {"cc":CustomerTrusted_node1744775159654, "stl":StepTrainerLanding_node1744777062263}, transformation_ctx = "filterstep_trainerdata_node1744087799450")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1744088089501 = glueContext.write_dynamic_frame.from_catalog(frame=filterstep_trainerdata_node1744087799450, database="stedi", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="StepTrainerTrusted_node1744088089501")

job.commit()
