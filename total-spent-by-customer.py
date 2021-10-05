from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmount")
sc = SparkContext(conf = conf)

def parseLines(line):
    fields = line.split(',')
    return (int(fields[0]),float(fields[2]))

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLines)
amounts = parsedLines.reduceByKey(lambda x,y: x + y)
results = amounts.collect();

for result in results:
    print(result)
