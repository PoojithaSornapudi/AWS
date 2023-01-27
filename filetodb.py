#Required libraries
import sys
from awsglue.utils import getResolvedOptions
import redshift_connector
import pandas as pd
import psycopg2
import sqlalchemy as sa
from sqlalchemy.engine.url import URL

conn=redshift_connector.connect(
		database='demodb',
 		host='host',
		password='password',
		port=port,
		user='username') 
		

#Reading data from redshift
cursor=conn.cursor()

#Params
target_table="tablename"
delimiter=","
file_format="csv"
path="s3://bucketname/outputfolder/"+file_format

copy_query = "COPY %s from '%s' iam_role 'arn:aws:iam::************:role/**************' delimiter '%s' format as %s;" %(target_table,path,delimiter,file_format)

cursor.execute(copy_query)
conn.commit()
cursor.close()
conn.close()


