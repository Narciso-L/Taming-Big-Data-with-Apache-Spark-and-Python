from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")
    
#Exercise
print("Friends by age: ")
people.select("age", "friends").groupBy("age").avg("friends").show()

#Sorted
people.select("age", "friends").groupBy("age").avg("friends").sort("age").show()

spark.stop()