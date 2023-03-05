import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import

## @params: [JOB_NAME, URL, WAREHOUSE, DB, SCHEMA, USERNAME, PASSWORD]
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
#args = getResolvedOptions(sys.argv, ['JOB_NAME', 'URL', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
java_import(spark._jvm, SNOWFLAKE_SOURCE_NAME)
## uj = sc._jvm.net.snowflake.spark.snowflake
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
sfOptions = {
"sfURL" : 'https://******.**-*****-*.aws.snowflakecomputing.com',
"sfUser" : 'sfuser',
"sfPassword" : 'password',
"sfDatabase" : 'database',
"sfSchema" : 'public',
"sfWarehouse" : 'COMPUTE_WH',
"application" : "AWSGlue"
}

## Read from a Snowflake table into a Spark Data Frame and writing to s3
df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "EMPL").load()
df.createOrReplaceTempView("employee")
df=spark.sql("select * from employee")
path="s3://test/snowflakes_to_s3/"
file_format='csv'
if bool(df.head(1)):
    df.coalesce(1).write.option("header","true").format(file_format).mode('overwrite').save(path)

    

