import sys
from pyspark import SparkConf , SparkContext

# count fruit function
def countFruit(inputPath,outputPath):
	conf = SparkConf().setMaster("local").setAppName("countFruit")

	sc = SparkContext(conf=conf)

	input = sc.textFile(inputPath)
	
	sourcePairRDD = input\
	.flatMap(lambda file: file.split('\n'))\
	.map(lambda line: line.split(':'))
	
	reversedPairRDD = sourcePairRDD\
	.flatMap(lambda pair: \
		[(fruit, pair[0]) for fruit in pair[1].split(',')])\
	.groupByKey()

	reversedPairRDD = reversedPairRDD.map(lambda p: [(p[0], p[1]),len(p[1])]).collect()
	
	maxRDD = max(reversedPairRDD, key=lambda x: x[1])[0]
	
	resultRDD = sc.parallelize((maxRDD[0], '\t'.join([item for item in maxRDD[1]])))

	resultRDD.saveAsTextFile(outputPath)

# main function
if __name__=='__main__':
	countFruit(sys.argv[1],sys.argv[2]);