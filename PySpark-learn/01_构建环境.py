from pyspark import SparkConf, SparkContext
import os
os.environ["JAVA_HOME"] = "D:/Program Files/Java"
# 创建类对象
conf = SparkConf().setMaster('local[*]').setAppName('1')
sc = SparkContext(conf=conf)
print(sc.version)
sc.stop()