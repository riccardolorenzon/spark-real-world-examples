import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("./book.txt")
words = input.flatMap(normalizeWords)
word_counts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).map(lambda(x,y): (y,x)).sortByKey()
word_counts = word_counts.collect()

for count, word  in word_counts:
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print cleanWord, count
