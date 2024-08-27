from pyspark import SparkConf, SparkContext
import os
import json
os.environ['JAVA_HOME'] = "D:/Program Files/Java"
os.environ['PYSPARK_PYTHON'] = "D:/Anaconda/python.exe"
os.environ['HADOOP_HOME'] = "C:/hadoop-3.0.0/hadoop-3.0.0"
# 创建Spark入口
conf = SparkConf().setMaster('local[*]').setAppName('1')
conf.set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
# 读入并处理数据
rdd = sc.textFile("G:/记事本数据表/search_log.txt")
rdd = rdd.map(lambda x: x.split("\t"))
# 打印输出热门搜索时段：top3

# rdd1= rdd.map(lambda x: (x[0][:2], 1))
# rdd1 = rdd1.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1)
# rdd1_list = rdd1.take(3)
# print(rdd1_list)

# 结果：[('20', 3479), ('23', 3087), ('21', 2989)]

# 打印热门搜索词top3

# rdd2 = rdd.map(lambda x: (x[2], 1))
# rdd2_list = rdd2.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(3)
# print(rdd2_list)

# 结果： [('scala', 2310), ('hadoop', 2268), ('博学谷', 2002)]

# 打印输出：黑马程序员在哪个时段被搜索最多
# rdd3 = rdd.filter(lambda x: x[2] == '黑马程序员')
# rdd3 = rdd3.map(lambda x: (x[0][:2], 1))
# rdd3 = rdd3.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1)
# rdd3_list = rdd3.take(1)
# print(rdd3_list)
# 结果22点：245次

# 将数据转换为json格式，写出为文件
# rdd4 = rdd.map(lambda data: {"time": data[0], "user_id": data[1],
#                              "keyword": data[2], "rank1": data[3], "rank2": data[4], "url": data[5]})
# rdd4 = rdd4.map(lambda x: json.dumps(x, ensure_ascii=False))
# rdd4.saveAsTextFile("G:/记事本数据表/search_log.JSON.txt")

# 分布式集群运行综合案例代码

# 关闭入口
sc.stop()


