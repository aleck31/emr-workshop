from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F

from pyspark.sql.types import DoubleType, FloatType, LongType, StructType, StructField, StringType, IntegerType

from pyspark.sql.functions import col, lit
#from datetime import datetime


spark = SparkSession \
        .builder \
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()    


#Variables 
DB_NAME = "test_db_0515"
TABLE_NAME = "stu_iceberg"

# 创建测试数据
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False)
])

data = [
    (1, "张三", 18),
    (2, "李四", 19),
    (3, "王五", 20),
    (4, "赵六", 21),
    (5, "钱七", 22)
]

# 创建DataFrame
df = spark.createDataFrame(data, schema)

# 将数据保存为临时表
df.createOrReplaceTempView("student_temp")

#Create the customer table in Iceberg 
spark.sql(f"""
    CREATE OR REPLACE TABLE dev.`{DB_NAME}`.`{TABLE_NAME}`(
        id             int,
        name           string,
        age            int
    )
    USING iceberg
    PARTITIONED BY (age)
    OPTIONS ('format-version'='2')
    """)

#Insert data into customer table
spark.sql(f"""
    INSERT INTO dev.`{DB_NAME}`.`{TABLE_NAME}` 
    SELECT * FROM student_temp
    """)

print("Iceberg表创建和数据导入完成!")
