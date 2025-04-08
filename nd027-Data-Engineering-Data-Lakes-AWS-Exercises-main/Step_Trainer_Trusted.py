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

# Script generated for node Step Trainer
StepTrainer_node1744087565697 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://cmulli/step_trainer/"], "recurse": True}, transformation_ctx="StepTrainer_node1744087565697")

# Script generated for node Customer Curated
CustomerCurated_node1744087689967 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1744087689967")

# Script generated for node Accerometer Trusted
AccerometerTrusted_node1744087753352 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccerometerTrusted_node1744087753352")

# Script generated for node filter step_trainer data
SqlQuery6155 = '''
select stl.* 
from stl 
join at on stl.sensorReadingTime = at.timeStamp
join cc on at.user = cc.email and stl.serialNumber = cc.serialNumber

'''
filterstep_trainerdata_node1744087799450 = sparkSqlQuery(glueContext, query = SqlQuery6155, mapping = {"stl":StepTrainer_node1744087565697, "cc":CustomerCurated_node1744087689967, "at":AccerometerTrusted_node1744087753352}, transformation_ctx = "filterstep_trainerdata_node1744087799450")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1744088089501 = glueContext.write_dynamic_frame.from_catalog(frame=filterstep_trainerdata_node1744087799450, database="stedi", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="StepTrainerTrusted_node1744088089501")

job.commit()