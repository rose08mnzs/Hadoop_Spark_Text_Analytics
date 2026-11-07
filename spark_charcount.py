from pyspark import SparkContext
sc = SparkContext("local[*]", "CharCount")

text = sc.textFile("hdfs:///gutenberg/*.txt")
chars = text.flatMap(lambda line: list(line)) \
           .map(lambda ch: (ch, 1)) \
           .reduceByKey(lambda a, b: a + b)
chars.saveAsTextFile("hdfs:///output_spark_charcount")
