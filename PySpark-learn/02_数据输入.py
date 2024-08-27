from pyspark import SparkConf, SparkContext
import os
os.environ["JAVA_HOME"] = "D:/Program Files/Java"
# 创建类对象
conf = SparkConf().setMaster('local[*]').setAppName('1')
sc = SparkContext(conf=conf)

# 传入数据
# rdd1 = sc.parallelize([1, 2, 3])
# rdd2 = sc.parallelize((1, 2, 3))
# rdd3 = sc.parallelize({1, 2, 3})
# rdd4 = sc.parallelize("shilianghan")
# rdd5 = sc.parallelize({"key1": "value1", "key2": "value2"})

# 打印需要使用collect方法
# print(rdd1.collect())
# print(rdd2.collect())
# print(rdd3.collect())
# print(rdd4.collect())
# print(rdd5.collect())

# 读取文件
rdd = sc.textFile("G:/记事本数据表/hello.txt")
print(rdd.collect())

# 关闭入口
sc.stop()