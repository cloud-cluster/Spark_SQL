from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql import Row


# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('/var/hive/warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# spark is an existing SparkSession
# spark.sql("CREATE TABLE IF NOT EXISTS stu111 (key INT, value STRING) row  format  delimited  fields   terminated  by  '\t'  lines terminated by '\n'")
# spark.sql("LOAD DATA LOCAL INPATH '/var/www/html/spark_sql/stu.txt' INTO TABLE stu111")

sc = spark.sparkContext
# sc.setLogLevel("INFO")
# Queries are expressed in HiveQL
# spark.sql("SELECT * FROM t_student").show()
spark.sql("select * from src").show()

# Record = Row("key", "value")
# recordsDF = spark.createDataFrame([Record(i, "val_" + str(i)) for i in range(1, 101)])
# recordsDF.createOrReplaceTempView("records")

# spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()