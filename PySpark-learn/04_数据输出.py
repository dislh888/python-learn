from pyspark import SparkConf, SparkContext
import os
import json
os.environ['JAVA_HOME'] = "D:/Program Files/Java"
os.environ['PYSPARK_PYTHON'] = "D:/Anaconda/python.exe"
os.environ['HADOOP_HOME'] = "C:/hadoop-3.0.0/hadoop-3.0.0"
# 创建类对象
conf = SparkConf().setMaster('local[*]').setAppName('1')
# conf.set("spark.default.parallelism", "1")  设置分区数
sc = SparkContext(conf=conf)

# 准备rdd
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd1 = sc.parallelize([1, 2, 3, 4, 5], numSlices=1)
rdd2 = sc.parallelize([("Hello", 3), ("Spark", 5), ("Hi", 7)], numSlices=1)
rdd3 = sc.parallelize([[1, 3, 5], [6, 7, 9], [11, 13, 11]], numSlices=1)


# collect算子，输出rdd为list对象
# rdd_list = rdd.collect()
# print(rdd_list, type(rdd_list))

# reduce算子：传入逻辑进行聚合
# num = rdd.reduce(lambda a, b: a + b)
# print(num, type(num))

# take算子：取前N个元素，返回list
# take_list = rdd.take(3)
# print(take_list, type(take_list))

# count算子
# num_count = rdd.count()
# print(f"rdd内有{num_count}个元素")

# 输出到文件
rdd1.saveAsTextFile("G:/记事本数据表/rdd1.txt")
rdd2.saveAsTextFile("G:/记事本数据表/rdd2.txt")
rdd3.saveAsTextFile("G:/记事本数据表/rdd3.txt")

# 修改分区数

# 法1：conf.set("spark.default.parallelism", "1")
# 法2：rdd1 = sc.parallelize([1, 2, 3, 4, 5], numSlices=1)
# 关闭链接
sc.stop()