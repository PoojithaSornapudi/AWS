import sys
import pyspark
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType,DoubleType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
args = getResolvedOptions(sys.argv,['TempDir'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

# Postgres - Construct JDBC connection options
pgconn = {
    "url": "jdbc:postgresql://host/database",
    "dbtable": "tablename",
    "user": "username",
    "password": "password"}
    
# Mysql - Construct JDBC connection options
mysqlconn = {
    "url": "jdbc:mysql://host/database",
    "dbtable": "tablename",
    "user": "username",
    "password": "password"}
    
# Redshift - Construct JDBC connection options
redshiftconn = {  
    "url": "jdbc:redshift://clusterendpoint:port/database",
    "dbtable": "redshift-table-name",
    "user": "username",
    "password": "password",
    "redshiftTmpDir": args["TempDir"],
    "aws_iam_role": "arn:aws:iam::*********:role/*******************"
}

logger.info("-------------Reading the data from the source database -------------")

file_format="csv"
source="postgres"
if source=="postgres":
    sourcedf = glueContext.create_dynamic_frame.from_options(connection_type="postgresql",
                                                          connection_options=pgconn)
    path="s3://bucketname/outputfolder/"+file_format+"/"                                                         
elif source=="mysql":
    sourcedf = glueContext.create_dynamic_frame.from_options(connection_type="mysql",
                                                          connection_options=mysqlconn)
    path="s3://bucketname/outputfolder/"+file_format+"/"
elif source=="redshift":
    sourcedf = glueContext.create_dynamic_frame_from_options("redshift", redshiftconn)
    
    path="s3://bucketname/outputfolder/"+file_format+"/"
else:
    sys.exit("Please provide a valid source")
    
src_df=sourcedf.toDF()   
df.createOrReplaceTempView("tablename")
df1 = spark.sql("SELECT * FROM tablename")

#Some transformations
df2 = df1.withColumn("col1",col("col1").cast(DateType())) \
               .withColumn("col2",col("col2").cast(DateType())) \
               .withColumn("col3",col("col3").cast(IntegerType())) \
               .withColumn("col4",col("col4").cast(DoubleType()))
                
df3 = df2.dropna()

logger.info("-------------Writing to S3 -------------")

if bool(df3.head(1)):
    df3.write.format(file_format).mode('overwrite').save(path)
else:
    sys.exit("No data to write")


