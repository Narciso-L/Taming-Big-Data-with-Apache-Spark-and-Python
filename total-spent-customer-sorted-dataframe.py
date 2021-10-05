from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("SpentByCustomer").getOrCreate()

schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("amount", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")

# Aggregate to find total amount for every customer
totAmountByCustomer = df.groupBy("customerID").agg(func.round(func.sum("amount"), 2).alias("total_spent"))
totAmountByCustomer.show()

#Sort
totAmountByCustomerSorted = totAmountByCustomer.sort("total_spent")
totAmountByCustomerSorted.show(totAmountByCustomerSorted.count())

spark.stop()
