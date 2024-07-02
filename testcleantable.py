# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession


def main(output_table):
    # 创建SparkSession
    spark = SparkSession.builder.appName("DataCleaning").enableHiveSupport().getOrCreate()

    # 读取数据湖中的表格信息
    query = "SELECT * FROM tid_sdi_ai4data.ai4data_enterprise_bak LIMIT 100"  # 仅读取前100行进行示例
    df = spark.sql(query)

    # 显示原始数据
    print("Original Data:")
    df.show()

    # 停止SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
