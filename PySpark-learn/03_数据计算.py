from pyspark import SparkConf, SparkContext
import os
import json
os.environ["JAVA_HOME"] = "D:/Program Files/Java"
os.environ['PYSPARK_PYTHON'] = "D:/Anaconda/python.exe"
# 创建类对象
conf = SparkConf().setMaster('local[*]').setAppName('1')
sc = SparkContext(conf=conf)

# # 准备一个rdd对象
# rdd = sc.parallelize([1, 2, 3, 4, 5])
# print(rdd.collect())


# map方法：遍历
#
#
# def func(data):
#     return data * 10
#
#
# rdd = rdd.map(func).map(lambda x: x + 5)
# print(rdd.collect())


# # flatMap方法：遍历且去嵌套
# rdd = sc.parallelize(["itheima itcast 666", "itheima itheima itcast", "python itheima"])
# rdd1 = rdd.map(lambda x: x.split(" "))
# print(rdd1.collect())
# rdd2 = rdd.flatMap(lambda x: x.split(" "))
# print(rdd2.collect())


# reduceByKey算子：rdd.reduce(func: (v, v) -> v)：计数
# rdd = sc.parallelize([('男', 99), ('男', 88), ('女', 99), ('女', 66)])
# rdd2 = rdd.reduceByKey(lambda a, b: a + b)
# print(rdd2.collect())


# 练习案例 ---wordcount
# rdd = sc.textFile("G:/记事本数据表/hello.txt")
# print(rdd.collect())
# rdd = rdd.flatMap(lambda x: x.split(" "))
# print(rdd.collect())
# rdd = rdd.map(lambda x: (x, 1))
# print(rdd.collect())
# rdd = rdd.reduceByKey(lambda a, b: a + b)
# print(rdd.collect())


# filter：过滤
# rdd.filter(func)    其中  func：T(泛型)---> bool(布尔)
# rdd = sc.parallelize([1, 2, 3, 4, 5])
# rdd = rdd.filter(lambda num: num % 2 == 0)
# print(rdd.collect())


# distinct：去重
# rdd.distinct()
# rdd = sc.parallelize([1, 1, 2, 3, 2, 4, 3, 5, 7, 4, 7, 9, 8, '1', '1'])
# rdd = rdd.distinct()
# print(rdd.collect())


# sortBy算子：排序
# rdd.sortBy(func, ascending=False, numPartitions=1)
# rdd = rdd.sortBy(lambda x: x[1], ascending=False, numPartitions=1)
# print(rdd.collect())


# 练习案例


# 数据处理
rdd = sc.textFile('G:/记事本数据表/orders.txt')
rdd = rdd.flatMap(lambda x: x.split("|"))
rdd = rdd.map(lambda x: json.loads(x))


# 各个城市销售额排名
rdd1 = rdd.map(lambda x: (x['areaName'], int(x['money'])))
rdd1 = rdd1.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1)
print(rdd1.collect())

# 全部城市的商品类别
rdd2 = rdd.map(lambda x: x['category']).distinct()
print(rdd2.collect())


# 北京市的商品类别
rdd3 = rdd.filter(lambda x: x['areaName'] == "北京").map(lambda x: x['category']).distinct()
print(rdd3.collect())


