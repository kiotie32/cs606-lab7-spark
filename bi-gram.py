import pyspark
from pyspark import SparkContext
sc =SparkContext.getOrCreate()

lines = sc.textFile("C:\\Users\\Bipin Kumar Mishr\\tapas data\\IIITG Lab assignments\\DATA ANALYTICS\\cs606-lab7\\cs606-lab7-spark-tapasm2027-master\\cs606-lab7-spark-tapasm2027-master\\pg100.txt")
lines.collect()
bigrams = lines.map(lambda s : s.split(" ")).flatMap(lambda s: [((s[i],s[i+1]),1) for i in range (0, len(s)-1)])
bigrams.collect()
counts = bigrams.reduceByKey(lambda x, y : x+y)
counts.collect()
