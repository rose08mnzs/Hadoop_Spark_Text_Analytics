from pyspark import SparkContext
import re

sc = SparkContext("local[*]", "WordCountClean")
WORD_RE = re.compile(r"[A-Za-z]+'?[A-Za-z]+|[A-Za-z]+")

text = sc.textFile("hdfs:///gutenberg/*.txt")
words = text.flatMap(lambda line: WORD_RE.findall(line.lower()))
counts = words.map(lambda w: (w, 1)).reduceByKey(lambda a,b: a+b)

# save to HDFS
counts.saveAsTextFile("hdfs:///output_spark_wordcount_clean")

# (optional) print top 30 locally for sanity
top = counts.map(lambda kv: (kv[1], kv[0])).sortByKey(ascending=False).take(30)
for c,w in top: print(c, w)
