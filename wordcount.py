# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession


def show(x):
    print(x)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <inputPath> <outputPath>")
        exit(-1)

    inputPath = sys.argv[1]
    outputPath = sys.argv[2]

    # 创建SparkSession
    spark = SparkSession.builder.appName("wordcount").getOrCreate()

    # 读取CSV文件
    df = spark.read.csv(inputPath, header=True, inferSchema=True)

    # 将DataFrame转化为RDD，并对每一行数据进行处理
    lines = df.rdd.map(lambda row: " ".join([str(x) for x in row]))

    # 数据扩展一百万倍
    expanded_lines = lines.flatMap(lambda line: [line] * (10 ** 6))

    # 每一行数据按照空格拆分  得到一个个单词
    words = expanded_lines.flatMap(lambda line: line.split(" "))

    # 将每个单词 组装成一个tuple 计数1
    pairWords = words.map(lambda word: (word, 1))

    # 使用3个分区 reduceByKey进行汇总
    result = pairWords.reduceByKey(lambda v1, v2: v1 + v2)

    # 打印结果
    result.foreach(lambda t: show(t))

    # 将结果保存到文件
    result.saveAsTextFile(outputPath)

    # 停止SparkSession
    spark.stop()
